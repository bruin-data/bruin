# MySQL

Bruin supports MySQL as a data platform for SQL assets and ingestion pipelines.

## Connection

Add a MySQL entry under `connections` in `.bruin.yml` using the following schema.

```yaml
connections:
  mysql:
    - name: "connection_name"
      username: "mysql_user"
      password: "XXXXXXXXXX"
      host: "mysql.somehost.com"
      port: 3306
      database: "analytics"
      driver: "pymysql"           # optional, defaults to pymysql
      ssl_ca_path: "path/to/ca.pem"       # optional
      ssl_cert_path: "path/to/cert.pem"   # optional
      ssl_key_path: "path/to/key.pem"     # optional
```

> [!TIP]
> If you plan to execute any SQL containing multiple statements (e.g. Bruin table materializations), ensure the connection allows multi-statements. When using the built-in MySQL client in Bruin this flag is automatically appended to the DSN.

## MySQL Assets

### `my.sql`

Executes a materialized MySQL SQL asset. See the [definition schema](../assets/definition-schema.md) for available parameters.

#### Example: Create and refresh a table

```bruin-sql
/* @bruin
name: warehouse.example
type: my.sql
materialization:
    type: table
@bruin */

SELECT
    id,
    country,
    name
FROM staging.customers
```

### `my.seed`

`my.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your MySQL database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the MySQL database.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

```yaml
name: dashboard.hello
type: my.seed

parameters:
    path: seed.csv
```

**Parameters**:

- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.

#### Examples: Load csv into a MySQL database

The examples below show how to load a CSV into a MySQL database.

```yaml
name: dashboard.hello
type: my.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

This operation will load the CSV into a table called `dashboard.hello` in the MySQL database.

### `my.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.

Checks if a table exists in MySQL, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `table`: `database.table` format, requires the database and table identifiers as a full name.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Examples

```yaml
# Check if a daily summary table exists
name: analytics.daily_summary
type: my.sensor.table
parameters:
    table: "analytics.daily_summary_{{ end_date | date_format('%Y%m%d') }}"
```

### `my.sensor.query`

Checks if a query returns any results in MySQL, runs by default every 30 seconds until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `query`: Query you expect to return any results.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Example: Check if data exists for a specific date

Checks if data is available in an upstream table for the end date of the run.

```yaml
name: analytics.daily_orders
type: my.sensor.query
parameters:
    query: select exists(select 1 from orders where order_date = "{{ end_date }}")
```

## CDC (Change Data Capture)

Bruin supports MySQL CDC via the `ingestr` asset type. CDC captures row-level changes (inserts, updates, deletes) via MySQL binary-log replication and replicates them to a destination. [Vitess](/ingestion/vitess) (VStream) and [PlanetScale](/ingestion/planetscale) (psdbconnect) are MySQL-compatible but use their own dedicated connection types — see their ingestion guides for CDC.

CDC is enabled by setting `cdc: "true"` on an ingestr asset with a MySQL source connection.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `cdc` | Yes | Set to `"true"` to enable CDC mode |
| `cdc_mode` | No | **Deprecated** — use `stream` instead. MySQL binary-log CDC only supports batch (read up to the current binlog position and exit), so leave `stream` unset; `stream: true` is rejected by ingestr |
| `cdc_server_id` | No | Replication server identifier for MySQL binary-log CDC |
| `cdc_dest_schema` | No | Destination schema to use for multi-table CDC runs |
| `incremental_strategy` | No | Defaults to `"merge"` when CDC is enabled. CDC assets must use `"merge"`; Bruin rejects other strategies. |

Requirements:

- For MySQL binary-log CDC, the server must have `log_bin=ON`, `binlog_format=ROW`, and `binlog_row_image=FULL`.
- Source tables must have primary keys, or `primary_key` must be set on the asset columns.
- Source tables must not contain `ENUM`, `SET`, or `BIT` columns.

> [!NOTE]
> When CDC is enabled, primary key columns do not need to be specified in the asset definition — they are determined automatically from the source table.

### Example: MySQL CDC

```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: my_mysql
  source_table: orders
  destination: bigquery
  cdc: "true"
```

For platform-specific details, see the [Vitess](/ingestion/vitess) and [PlanetScale](/ingestion/planetscale) ingestion guides.
