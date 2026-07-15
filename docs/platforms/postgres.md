# PostgreSQL

Bruin supports PostgreSQL as a data platform.

## Connection

In order to set up a PostgreSQL connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      postgres:
        - name: "connection_name"
          username: "pguser"
          password: "XXXXXXXXXX"
          host: "pghost.somedomain.com"
          port: 5432
          database: "dev"
          ssl_mode: "allow" # optional
          schema: "schema_name" # optional
          pool_max_conns: 5 # optional
```

> [!NOTE]
> `ssl_mode` should be one of the modes describe in the [documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION).

## PostgreSQL Assets

### `pg.sql`

Runs a materialized Postgres asset or an sql script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization

```bruin-sql
/* @bruin
name: events.install
type: pg.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a Postgres script

```bruin-sql
/* @bruin
name: events.install
type: pg.sql
@bruin */

create temp table first_installs as
select distinct on (user_id)
    user_id, 
    ts as install_ts,
    platform,
    country
from analytics.events
where event_name = 'install'
order by user_id, ts;

create table if not exists events.install as
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel
from first_installs as i
join marketing.attribution as a
    using(user_id)
```

### `pg.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.

Checks if a table exists in Postgres, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `table`: `schema_id.table_id` or (for default schema `public`) `table_id` format.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

### `pg.sensor.query`

Checks if a query returns any results in Postgres, runs by default every 30 seconds until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `query`: Query you expect to return any results
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.

```yaml
name: analytics_123456789.events
type: pg.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}")
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.

```yaml
name: analytics_123456789.events
type: pg.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}")
```

### `pg.seed`

`pg.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your PostgreSQL database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the PostgreSQL database.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

**Parameters**:

- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.

#### Examples: Load csv into a Postgres database

The examples below show how to load a CSV into a PostgreSQL database.

```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

## CDC (Change Data Capture)

Bruin supports PostgreSQL CDC via the `ingestr` asset type. CDC uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) to capture row-level changes (inserts, updates, deletes) and replicate them to a destination.

### Prerequisites

Your source PostgreSQL database must be configured for logical replication:

- `wal_level` set to `logical`
- A [publication](https://www.postgresql.org/docs/current/sql-createpublication.html) created for the tables you want to replicate
- A [replication slot](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS) allocated for the CDC consumer

Refer to the [PostgreSQL logical replication documentation](https://www.postgresql.org/docs/current/logical-replication.html) for detailed setup instructions.

### Parameters

CDC is enabled by setting `cdc: "true"` on an `ingestr` asset with a PostgreSQL source connection.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `cdc` | Yes | Set to `"true"` to enable CDC mode |
| `stream` | No | Set to `true` for continuous (real-time) streaming. Omit for batch replication (read up to the current WAL position and exit) |
| `cdc_mode` | No | **Deprecated** — use `stream` instead. `cdc_mode: stream` is equivalent to `stream: true` |
| `cdc_publication` | No | Name of the PostgreSQL publication to use |
| `cdc_slot` | No | Name of the PostgreSQL replication slot to use |
| `cdc_dest_schema` | No | Schema to use when running multi-table CDC |
| `cdc_stream_metrics_addr` | No | Address to serve streaming metrics on, such as `127.0.0.1:6060`. Only valid for a streaming asset (`stream: true`) |
| `cdc_stream_flush_interval` | No | How often buffered records are written to the destination, such as `30s`. Takes precedence over `flush_interval` |
| `cdc_stream_flush_records` | No | Number of buffered records that triggers a write to the destination. Takes precedence over `flush_records` |
| `source_table` | Yes | Source table in `schema.table` format, or `"*"` to replicate all tables in the publication |
| `incremental_strategy` | No | Defaults to `"merge"` when CDC is enabled. CDC assets must use `"merge"`; Bruin rejects other strategies. |

> [!NOTE]
> When CDC is enabled, primary key columns do not need to be specified in the asset definition — they are determined automatically from the source table.

### Examples

#### Basic CDC replication
```yaml
name: public.users
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.users
  destination: bigquery
  cdc: "true"
```

#### CDC with explicit publication and slot
```yaml
name: public.users
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.users
  destination: bigquery
  cdc: "true"
  cdc_publication: my_publication
  cdc_slot: my_slot
```

#### CDC with stream mode
```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.orders
  destination: bigquery
  cdc: "true"
  stream: true
```

A streaming CDC asset (`stream: true`) runs continuously, so it is excluded from a normal `bruin run` and is launched on its own:

```bash
bruin run --stream assets/public.orders.asset.yml
```

The stream runs in the foreground until you stop it with `Ctrl+C`, then flushes and exits cleanly. See [Streaming assets](../assets/ingestr.md#streaming-assets) for the full behaviour and restrictions.

#### Tuning and observing a stream
The `cdc_stream_*` parameters configure a running stream. `cdc_stream_flush_interval` and `cdc_stream_flush_records` control how often buffered changes reach the destination, and `cdc_stream_metrics_addr` serves replication lag, rows synced, and the last synced timestamp over HTTP for as long as the stream runs. The metrics are [expvar](https://pkg.go.dev/expvar) variables served at `/debug/vars`, and Postgres reports its lag as `bytes_behind`: the WAL the source has produced but the replication slot has not confirmed as durable.

Nothing is served unless `cdc_stream_metrics_addr` is set, and it requires a streaming asset (`stream: true`) — ingestr rejects the address otherwise.

```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.orders
  destination: bigquery
  cdc: "true"
  stream: true
  cdc_stream_flush_interval: 30s
  cdc_stream_flush_records: 10000
  cdc_stream_metrics_addr: 127.0.0.1:6060
```

```bash
curl -s localhost:6060/debug/vars | jq '.ingestr_replication, .ingestr_stream_tables'
```

#### Wildcard CDC — replicate all tables
When `source_table` is set to `"*"`, all tables in the publication are replicated to the destination.

```yaml
name: public.all_tables
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: "*"
  destination: bigquery
  cdc: "true"
```

### `pg.source`

Defines PostgreSQL source assets for documenting existing tables and views in your PostgreSQL database. These assets are no-op (they don't execute), but are useful for:

- Documenting existing PostgreSQL tables and views
- Adding column descriptions and metadata
- Establishing lineage relationships
- Query preview functionality in the VSCode extension

#### Example: Document an existing PostgreSQL table

```yaml
name: public.orders
type: pg.source
description: "All customer orders with their current status and totals"
connection: postgres-default

tags:
  - ecommerce
  - transactions
domains:
  - sales

meta:
  business_owner: "Sales Team"
  data_steward: "data-eng@company.com"
  refresh_frequency: "daily"

depends:
  - public.customers

columns:
  - name: order_id
    type: "SERIAL"
    description: "Auto-incrementing unique identifier for each order"
  - name: customer_id
    type: "INTEGER"
    description: "Foreign key referencing the customers table"
  - name: total_amount
    type: "NUMERIC(12,2)"
    description: "Total monetary value of the order"
  - name: order_date
    type: "TIMESTAMPTZ"
    description: "Timestamp when the order was placed"
  - name: status
    type: "VARCHAR(50)"
    description: "Current order status such as pending, shipped, or delivered"
```
