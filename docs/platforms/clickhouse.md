# Clickhouse

[Clickhouse](https://clickhouse.com/) is a high-performance, column-oriented SQL database management system for online analytical processing.
Bruin supports Clickhouse as both a source and a destination.

## Connection

In order to set up a Clickhouse connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
connections:
    clickhouse:
        - name: "connection_name"
          username: "clickhouse"
          password: "XXXXXXXXXX"
          host: "some-clickhouse-host.somedomain.com"   
          port: 9440
          database: "dev" #Optional for other assets. Ignored when using ClickHouse as an ingestr destination/source, as ingestr takes the database name from the asset file. 
          http_port: 8443 #Only specify if you are using ClickHouse as ingestr destination, by default it is 8443.
          secure: 1 #Required for ClickHouse Cloud
```

## Ingestr Assets

After adding connection in `bruin.yml`. To ingest data to clickhouse, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., stripe_ingestion.yml) inside the assets folder and add the following content:

###

```yaml
name: publicDB.stripe
type: ingestr

parameters:
  source_connection: stripe-default
  source_table: 'events'
  destination: clickhouse
```

In this case, the Clickhouse database is `publicDB`. Please ensure that the necessary permissions are granted to the user. For more details on obtaining credentials and setting up permissions, you can refer to this [guide](https://dlthub.com/docs/dlt-ecosystem/destinations/clickhouse#2-setup-clickhouse-database)

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
| `engine` | The ClickHouse table engine to use (e.g. `merge_tree`, `replicated_merge_tree`). |
| `engine.<setting>` | Engine-specific settings passed directly to ClickHouse (e.g. `engine.index_granularity`). |

## Clickhouse Assets

### `clickhouse.sql`

Runs a materialized clickhouse asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

### Materialization and incremental strategies

`clickhouse.sql` supports table and view materializations. For a table with no explicit strategy, Bruin uses `create+replace`.

| Strategy | Support | How Bruin executes it |
| --- | --- | --- |
| `create+replace` | Supported | Creates a temporary table from the asset query, drops the target table, then renames the temporary table to the target name. Requires `columns` and exactly one column marked `primary_key: true`. |
| `append` | Supported | Runs `INSERT INTO <target> <asset query>`. Bruin does not add filtering or deduplicate rows; make the asset query select only the new rows. |
| `delete+insert` | Supported | Refreshes the values returned for an `incremental_key`: it writes the query result to a temporary table, deletes target rows whose incremental-key value occurs in that table, inserts the temporary-table rows, then drops the temporary table. Requires `incremental_key`, `columns`, and exactly one `primary_key: true` column. |
| `time_interval` | Supported | On a normal run, deletes target rows in the requested date or timestamp interval, then inserts the asset query result. Requires `incremental_key`, `time_granularity` (`date` or `timestamp`), and an existing target table. The asset query must filter itself to the same interval. A `--full-refresh` runs `create+replace` instead, which requires `columns` and exactly one `primary_key: true` column. |
| `truncate+insert` | Supported | Truncates the existing table, then inserts the asset query result. This is a full-table refresh that preserves the table definition; it is not an incremental strategy. |
| `ddl` | Supported | Creates the table if it does not already exist from the defined columns, primary key, and optional `partition_by`. Do not include a query in a DDL asset. |
| `merge`, `scd2_by_column`, `scd2_by_time` | Not supported | Use `delete+insert`, `time_interval`, or an explicit ClickHouse SQL implementation instead. |

Create the target table with `create+replace` or `ddl` before its first `append`, `delete+insert`, `time_interval`, or `truncate+insert` run. `create+replace`, including a `--full-refresh` of a `time_interval` asset, requires `columns` and exactly one `primary_key: true` column. The primary key defines the temporary table; Bruin does not use it to deduplicate or merge rows.

View materializations support only the default strategy, which creates or replaces the view. Table-only strategies, including all incremental strategies, are not supported for views.

> [!NOTE]
> ClickHouse statements are executed one at a time and are not wrapped in a transaction. In particular, a failed `delete+insert`, `time_interval`, or `truncate+insert` run can leave the target table between steps. Use idempotent, date- or partition-bounded queries and rerun the asset to recover.

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

Use `time_interval` for a known run window. Bruin uses the run's `start_date` and `end_date` values for a `date` key, or `start_timestamp` and `end_timestamp` for a `timestamp` key. The bounds are inclusive.

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

Bruin deletes `analytics.daily_orders` rows whose `dt` falls within that interval, then inserts the query result. It does not automatically add the `WHERE` clause shown above.

#### Full refresh behavior

Running `bruin run --full-refresh` changes every ClickHouse table materialization except `ddl` to `create+replace`. This includes `time_interval`: it does **not** use interval-based deletion during a full refresh. The rebuild creates a temporary table, drops the target, then renames the temporary table. It requires `columns` and exactly one `primary_key: true` column. A `ddl` asset remains `CREATE TABLE IF NOT EXISTS` during a full refresh.

### Column data types

Bruin does not translate or validate ClickHouse column types against its own allowlist. For `ddl` materializations, it passes `columns[].type` through to ClickHouse. `precision`/`scale` or `length` are added to an unparameterized type when you provide them separately. ClickHouse is therefore the authority for whether a type is available on your server version and configuration; see its [data type reference](https://clickhouse.com/docs/sql-reference/data-types) for the complete, current list.

For `create+replace`, `delete+insert`, and a `--full-refresh`, ClickHouse derives the temporary table's column types from the asset query's `SELECT` result. For `append`, `truncate+insert`, and normal `time_interval` runs, ClickHouse uses the existing target table's schema.

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
name: average_Rating
type: clickhouse.sql
materialization:
    type: table
@bruin */

SELECT 
    driver_id, 
    AVG(rating) AS average_rating 
FROM trips 
GROUP BY driver_id 
ORDER BY average_rating DESC;
```

### `clickhouse.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.

Checks if a table exists in Clickhouse, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `table`: `database.table_id` or (if using `default` database or a database specified in config file) `table_id` format.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

### `clickhouse.sensor.query`

Checks if a query returns any results in Clickhouse, runs by default every 30 seconds until this query returns any results.

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

`clickhouse.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Clickhouse database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Clickhouse database.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

```yaml
name: dashboard.hello
type: clickhouse.seed

parameters:
    path: seed.csv
```

**Parameters**:

- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.

#### Examples: Load csv into a Clickhouse database

The examples below show how to load a CSV into a Clickhouse database:

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

Defines Clickhouse source assets for documenting existing tables and views in your Clickhouse database. These assets are no-op (they don't execute), but are useful for:

- Documenting existing Clickhouse tables and views
- Adding column descriptions and metadata
- Establishing lineage relationships
- Query preview functionality in the VSCode extension

#### Example: Document an existing Clickhouse table

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
