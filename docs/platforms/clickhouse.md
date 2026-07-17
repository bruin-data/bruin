# ClickHouse

[ClickHouse](https://clickhouse.com/) is a high-performance, column-oriented SQL database management system for online analytical processing.
Bruin supports ClickHouse as both a source and a destination.

## Connection

To set up a ClickHouse connection, add a configuration item to `connections` in `.bruin.yml`:

```yaml
connections:
  clickhouse:
    - name: "connection_name"
      username: "clickhouse"
      password: "XXXXXXXXXX"
      host: "some-clickhouse-host.somedomain.com"
      port: 9440
      database: "dev" # Default database for direct ClickHouse assets and unqualified seeds.
      http_port: 8443 # Optional; used only for ingestr and defaults to 8443.
      secure: 1 # Set to 1 for ClickHouse Cloud or another TLS connection.
```

Bruin uses `database` for direct ClickHouse assets and unqualified `clickhouse.seed` asset names. For an `ingestr` asset, the generated ClickHouse URI does not include this database: use `database.table` in the asset `name` when ClickHouse is the destination, or in `source_table` when it is the source. An unqualified ingestr table uses ClickHouse's `default` database.

## Ingestr Assets

After adding a connection in `.bruin.yml`, create an [asset configuration](/assets/ingestr#asset-structure) file such as `stripe_ingestion.asset.yml` inside the `assets` directory. This file defines the flow from the source to the destination:

```yaml
name: publicDB.stripe
type: ingestr

parameters:
  source_connection: stripe-default
  source_table: 'events'
  destination: clickhouse
```

In this case, the ClickHouse database is `publicDB`. Ensure the configured user has the required permissions. For more details on credentials and permissions, see this [guide](https://dlthub.com/docs/dlt-ecosystem/destinations/clickhouse#2-setup-clickhouse-database).

### Engine Settings

You can configure the ClickHouse table engine and its settings via the `parameters` block. Use `engine` to set the table engine and `engine.<setting>` to pass engine-specific settings.

```yaml
name: publicDB.events
type: ingestr

parameters:
  source_connection: stripe-default
  source_table: 'events'
  destination: clickhouse
  engine: merge_tree
  engine.index_granularity: 8125
```

| Parameter | Description |
|-----------|-------------|
| `engine` | The ClickHouse table engine: `merge_tree`, `replacing_merge_tree`, `shared_merge_tree`, or `replicated_merge_tree`. If omitted, ingestr uses `ReplacingMergeTree()` when the schema has a primary key and `MergeTree()` otherwise. |
| `engine.<setting>` | An engine setting rendered in ClickHouse's `SETTINGS` clause (e.g. `engine.index_granularity`). |

### ClickHouse-native column hints

When loading into ClickHouse, `columns` can use supported ClickHouse-native type syntax. Bruin converts these declarations into ingestr-compatible type hints; it does not copy them directly into destination DDL. Parameterized types such as `DateTime64(3)` and `FixedString(16)` are supported, and `Nullable(T)` and `LowCardinality(T)` resolve to the inner type.

For an `ingestr` asset, set `parameters.enforce_schema: true` to emit the hints. A `clickhouse.seed` asset emits hints for declared columns by default; set `parameters.enforce_schema: false` to opt out.

```yaml
name: publicDB.events
type: ingestr

parameters:
  source_connection: stripe-default
  source_table: events
  destination: clickhouse
  enforce_schema: true

columns:
  - name: event_id
    type: UInt64
  - name: occurred_at
    type: DateTime64(3, 'UTC')
  - name: properties
    type: Nullable(String)
```

## ClickHouse Assets

### `clickhouse.sql`

Runs a materialized ClickHouse asset or an SQL script. An unmaterialized asset can run statements that return no rows, such as `ALTER`, `INSERT`, or `OPTIMIZE`, as well as queries whose result is discarded. For detailed parameters, see the [Definition Schema](../assets/definition-schema.md).

### Materialization and incremental strategies

`clickhouse.sql` supports table and view materializations. For a table with no explicit strategy, Bruin uses `create+replace`.

| Strategy | Support | How Bruin executes it |
| --- | --- | --- |
| `create+replace` | Supported | Runs `CREATE OR REPLACE TABLE <target> PRIMARY KEY <key> AS <asset query>`. Requires `columns` and exactly one column marked `primary_key: true`. |
| `append` | Supported | Runs `INSERT INTO <target> <asset query>`. Bruin does not add filtering or deduplicate rows; make the asset query select only the new rows. |
| `delete+insert` | Supported | Refreshes the values returned for an `incremental_key`: it writes the query result to a temporary table, deletes target rows whose incremental-key value occurs in that table, inserts the temporary-table rows, then drops the temporary table. Requires `incremental_key`, `columns`, and exactly one `primary_key: true` column. |
| `time_interval` | Supported | On a normal run, deletes target rows in the requested date or timestamp interval, then inserts the asset query result with `SETTINGS insert_deduplicate = 0` so a rerun of the same interval is not suppressed by ClickHouse insert deduplication. Requires `incremental_key`, `time_granularity` (`date` or `timestamp`), and an existing target table. The asset query must filter itself to the same interval. A `--full-refresh` runs `create+replace` instead, which requires `columns` and exactly one `primary_key: true` column. |
| `truncate+insert` | Supported | Truncates the existing table, then inserts the asset query result. This is a full-table refresh that preserves the table definition; it is not an incremental strategy. |
| `ddl` | Supported | Creates the table if it does not already exist from the defined columns, primary key, and optional `partition_by`. Do not include a query in a DDL asset. |
| `merge`, `scd2_by_column`, `scd2_by_time` | Not supported | Use `delete+insert`, `time_interval`, or an explicit ClickHouse SQL implementation instead. |

Create the target table with `create+replace` or `ddl` before its first `append`, `delete+insert`, `time_interval`, or `truncate+insert` run. `create+replace`, including a `--full-refresh` of a `time_interval` asset, requires `columns` and exactly one `primary_key: true` column. The primary key is used in the `CREATE OR REPLACE TABLE` statement; Bruin does not use it to deduplicate or merge rows.

View materializations support only the default strategy, which creates or replaces the view. Table-only strategies, including all incremental strategies, are not supported for views.

> [!NOTE]
> ClickHouse statements are executed one at a time and are not wrapped in a transaction. `create+replace` is a single `CREATE OR REPLACE TABLE` statement, but a failed `delete+insert`, `time_interval`, or `truncate+insert` run can leave the target table between steps. Use idempotent, date- or partition-bounded queries and rerun the asset to recover.

#### `delete+insert` example

Use `delete+insert` when the query returns complete replacements for one or more incremental-key values. For example, this refreshes every `dt` returned by the query:

```bruin-sql
/* @bruin
name: analytics.daily_orders
type: clickhouse.sql
materialization:
  type: table
  strategy: delete+insert
  incremental_key: dt
columns:
  - name: order_id
    type: UInt64
    primary_key: true
  - name: dt
    type: Date
@bruin */

SELECT
  order_id,
  toDate(created_at) AS dt
FROM raw.orders
WHERE toDate(created_at) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

The temporary table is created in the same database as the target. Bruin deletes existing `analytics.daily_orders` rows for the distinct `dt` values in that temporary table, then inserts the replacement rows.

#### `time_interval` example

Use `time_interval` for a known run window. Bruin uses the run's `start_date` and `end_date` values for a `date` key, or `start_timestamp` and `end_timestamp` for a `timestamp` key. The bounds are inclusive. Timestamp delete bounds are rendered with microsecond precision and no time-zone suffix.

```bruin-sql
/* @bruin
name: analytics.daily_orders
type: clickhouse.sql
materialization:
  type: table
  strategy: time_interval
  incremental_key: dt
  time_granularity: date
@bruin */

SELECT
  order_id,
  toDate(created_at) AS dt
FROM raw.orders
WHERE toDate(created_at) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

Bruin deletes `analytics.daily_orders` rows whose `dt` falls within that interval, then inserts the query result with `SETTINGS insert_deduplicate = 0`. This lets a rerun reinsert the interval after deletion, including on ClickHouse deployments that would otherwise deduplicate the repeated insert. Bruin does not automatically add the `WHERE` clause shown above.

#### Full refresh behavior

Running `bruin run --full-refresh` changes every ClickHouse table materialization except `ddl` to `create+replace`, unless the asset has `full_refresh_restricted: true` (or its `refresh_restricted` alias). This includes `time_interval`: it does **not** use interval-based deletion during a full refresh. The rebuild runs `CREATE OR REPLACE TABLE` and requires `columns` and exactly one `primary_key: true` column. A `ddl` asset remains `CREATE TABLE IF NOT EXISTS` during a full refresh.

### Column data types

Bruin does not translate or validate ClickHouse column types against its own allowlist. For `ddl` materializations, it passes `columns[].type` through to ClickHouse. `precision`/`scale` or `length` are added to an unparameterized type when you provide them separately. ClickHouse is therefore the authority for whether a type is available on your server version and configuration; see its [data type reference](https://clickhouse.com/docs/sql-reference/data-types) for the complete, current list.

For `create+replace`, `delete+insert`, and a `--full-refresh`, ClickHouse derives a new table's column types from the asset query's `SELECT` result. For `append`, `truncate+insert`, and normal `time_interval` runs, ClickHouse uses the existing target table's schema.

Common ClickHouse column types include:

| Family | Types and examples |
| --- | --- |
| Integers | `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`; `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256` |
| Floating point and exact numeric | `Float32`, `Float64`, `BFloat16`, `Decimal(P, S)` |
| Text and categorical | `String`, `FixedString(N)`, `Enum8(...)`, `Enum16(...)`, `LowCardinality(String)` |
| Date and time | `Date`, `Date32`, `DateTime`, `DateTime('UTC')`, `DateTime64(P, 'UTC')` |
| Scalar identifiers | `Bool`, `UUID`, `IPv4`, `IPv6` |
| Nullable and composite | `Nullable(T)`, `Array(T)`, `Tuple(...)`, `Map(K, V)`, `Nested(...)` |
| Version- or setting-dependent types | `JSON`, `Dynamic`, `Variant(...)`, `Time`, `Time64`, `QBit`, geo types, `AggregateFunction(...)`, `SimpleAggregateFunction(...)` |

For a DDL asset, use ClickHouse-native type syntax directly:

```bruin-sql
/* @bruin
name: analytics.events
type: clickhouse.sql
materialization:
  type: table
  strategy: ddl
columns:
  - name: event_id
    type: UInt64
    primary_key: true
  - name: occurred_at
    type: DateTime64(3, 'UTC')
  - name: amount
    type: Decimal
    precision: 12
    scale: 2
  - name: tags
    type: Array(String)
  - name: attributes
    type: Map(String, String)
  - name: payload
    type: Nullable(String)
@bruin */
```

This renders `amount` as `Decimal(12, 2)` and preserves the parameterized type declarations exactly as written.

### Examples

Create a view to determine the top 10 earning drivers in a taxi company:

```bruin-sql
/* @bruin
name: highest_earning_drivers
type: clickhouse.sql
materialization:
  type: view
@bruin */

SELECT 
    driver_id, 
    SUM(fare_amount) AS total_earnings 
FROM trips 
GROUP BY driver_id 
ORDER BY total_earnings DESC 
LIMIT 10;
```

View Top 5 Customers by Spending:

```bruin-sql
/* @bruin
name: top_five_customers
type: clickhouse.sql
materialization:
  type: view
@bruin */

SELECT 
    customer_id, 
    SUM(fare_amount) AS total_spent 
FROM trips 
GROUP BY customer_id 
ORDER BY total_spent DESC 
LIMIT 5;
```

Table with average driver rating:

```bruin-sql
/* @bruin
name: average_rating
type: clickhouse.sql
materialization:
  type: table
columns:
  - name: driver_id
    type: UInt64
    primary_key: true
  - name: average_rating
    type: Float64
@bruin */

SELECT 
    driver_id, 
    AVG(rating) AS average_rating 
FROM trips 
GROUP BY driver_id 
ORDER BY average_rating DESC;
```

#### Merge materialization

ClickHouse has no `MERGE INTO` statement, so Bruin implements the `merge` strategy with a delete+insert pattern keyed on the asset's primary key column(s):

- the query result is staged in a temporary table
- rows in the target whose primary key matches a staged row are deleted
- the staged rows are inserted into the target
- the temporary table is dropped

This upserts rows by primary key: existing rows are replaced and new rows are added, while untouched rows remain. The optional `incremental_predicate` is appended to the delete condition to scope which target rows are considered for replacement.

Merge assets must declare `columns` and at least one `primary_key` column. Composite primary keys are supported. On a full refresh, Bruin falls back to `create+replace` (creating the table from the query result) so the target exists for subsequent incremental merges.

Here's a sample asset with `merge` materialization:

```bruin-sql
/* @bruin
name: dashboard.drivers_summary
type: clickhouse.sql

materialization:
    type: table
    strategy: merge

columns:
  - name: driver_id
    type: integer
    primary_key: true
  - name: total_earnings
    type: float
  - name: average_rating
    type: float
@bruin */

SELECT
    driver_id,
    SUM(fare_amount) AS total_earnings,
    AVG(rating) AS average_rating
FROM trips
GROUP BY driver_id;
```

### `clickhouse.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.

Checks whether a table exists in ClickHouse. The default sensor mode is `once`, which checks once and fails if the table is unavailable. Run with `bruin run --sensor-mode wait` to retry every 30 seconds by default until it becomes available.

```yaml
name: upstream_table_available
type: clickhouse.sensor.table
parameters:
  table: database.table
  poke_interval: 30 # Optional
  timeout: 24h # Optional
```

**Parameters**:

- `table`: `database.table_id` or, when using the `default` database or a database specified in the connection, `table_id`.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

### `clickhouse.sensor.query`

Checks a ClickHouse query that returns exactly one boolean or numeric scalar. The sensor succeeds when the result is `true` or greater than zero. The default sensor mode is `once`; run with `bruin run --sensor-mode wait` to retry every 30 seconds by default until it succeeds.

```yaml
name: upstream_data_available
type: clickhouse.sensor.query
parameters:
  query: SELECT exists(SELECT 1 FROM upstream_table)
  poke_interval: 30 # Optional
  timeout: 24h # Optional
```

**Parameters**:

- `query`: A query that returns exactly one boolean or numeric scalar, such as `SELECT exists(...)` or `SELECT count() ...`.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.

```yaml
name: analytics_123456789.events
type: clickhouse.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}")
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.

```yaml
name: analytics_123456789.events
type: clickhouse.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}")
```

### `clickhouse.seed`

`clickhouse.seed` represents data prepared outside the pipeline and loads it into ClickHouse. Local seed files can be CSV, Parquet (`.parquet` or `.pq`), JSON, JSONL/NDJSON, or Avro. Bruin infers the format from a known file extension unless you set `file_type` explicitly; an unknown or missing extension falls back to CSV.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

```yaml
name: dashboard.hello
type: clickhouse.seed

parameters:
  path: seed.csv
  # file_type: csv # Optional; supported values are csv, parquet (or pq), json, jsonl, ndjson, and avro.
```

**Parameters**:

- `path`: A local file path, relative to the asset definition file, or an HTTP/HTTPS URL passed to ingestr unchanged.
- `file_type`: Optional format for a local file: `csv`, `parquet` (or `pq`), `json`, `jsonl`, `ndjson`, or `avro`. When omitted, Bruin infers it from a known file extension and otherwise uses CSV.

When a seed declares `columns`, Bruin emits ClickHouse-aware ingestr type hints by default. See [ClickHouse-native column hints](#clickhouse-native-column-hints) for supported type syntax and how to opt out.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.

#### Example: Load a CSV into a ClickHouse database

The example below loads a CSV into a ClickHouse database:

```yaml
name: dashboard.hello
type: clickhouse.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

### `clickhouse.source`

Defines ClickHouse source assets for documenting existing tables and views in your ClickHouse database. These assets are no-op (they don't execute), but are useful for:

- Documenting existing ClickHouse tables and views
- Adding column descriptions and metadata
- Establishing lineage relationships
- Query preview functionality in the VSCode extension

#### Example: Document an existing ClickHouse table

```yaml
name: analytics.page_views
type: clickhouse.source
description: "Page view events tracked across all web properties"
connection: clickhouse-default

tags:
  - analytics
  - web
  - events
domains:
  - web-analytics

meta:
  business_owner: "Data Team"
  data_steward: "data@company.com"
  refresh_frequency: "real-time"

depends:
  - analytics.users

columns:
  - name: view_id
    type: "UInt64"
    description: "Unique identifier for each page view"

  - name: user_id
    type: "String"
    description: "Identifier of the user who viewed the page"

  - name: page_url
    type: "String"
    description: "URL of the page that was viewed"

  - name: view_timestamp
    type: "DateTime"
    description: "Timestamp when the page was viewed"

  - name: duration_seconds
    type: "UInt32"
    description: "Time spent on the page in seconds"
```
