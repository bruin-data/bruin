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
      read_only: false
      # cluster: "analytics_cluster" # Optional; see self-hosted clusters below.
```

### Read-only connections

Set `read_only: true` to run SQL queries in read-only mode (default: `false`). Ingestr assets and seeds do not support this option.

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

### Ingestr destination engine settings

For `ingestr` assets loading into ClickHouse, configure the table engine and its settings via the `parameters` block. Use `engine` to set the table engine and `engine.<setting>` to pass engine-specific settings. These parameters do not configure native `clickhouse.sql` assets; those use the [`clickhouse` block](#native-sql-table-definitions).

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

`clickhouse.sql` supports table and view materializations. For a table with no explicit strategy, Bruin uses `create+replace`. The following table describes connections without `cluster`, including ClickHouse Cloud. See [self-hosted clusters](#self-hosted-clusters) for cluster-specific behavior and restrictions.

| Strategy | Support | How Bruin executes it |
| --- | --- | --- |
| `create+replace` | Supported | Runs `CREATE OR REPLACE TABLE <target> ... AS <asset query>` with the configured table options. If both `clickhouse.engine` and `clickhouse.order_by` are omitted, requires `columns` with at least one column marked `primary_key: true`. |
| `append` | Supported | Runs `INSERT INTO <target> <asset query>`. Bruin does not add filtering or deduplicate rows; make the asset query select only the new rows. |
| `delete+insert` | Supported | Refreshes the values returned for an `incremental_key`: it writes the query result to a temporary table, deletes target rows whose incremental-key value occurs in that table, inserts the temporary-table rows, then drops the temporary table. Requires `incremental_key`, `columns`, and exactly one `primary_key: true` column. |
| `time_interval` | Supported | On a normal run, deletes target rows in the requested date or timestamp interval, then inserts the asset query result with `SETTINGS insert_deduplicate = 0` so a rerun of the same interval is not suppressed by ClickHouse insert deduplication. Requires `incremental_key`, `time_granularity` (`date` or `timestamp`), and an existing target table. The asset query must filter itself to the same interval. A `--full-refresh` runs `create+replace` with its table options and key requirements. |
| `truncate+insert` | Supported | Truncates the existing table, then inserts the asset query result. This is a full-table refresh that preserves the table definition; it is not an incremental strategy. |
| `ddl` | Supported | Creates the table if it does not already exist from the defined columns, primary key, optional `materialization.partition_by`, and `clickhouse` table options. Do not include a query in a DDL asset. |
| `merge` | Supported | Stages the asset query in a MergeTree table, deletes target rows matching the staged primary keys, then inserts the staged rows. Requires `columns` with at least one column marked `primary_key: true`. |
| `scd2_by_column`, `scd2_by_time` | Not supported | Use `delete+insert`, `time_interval`, or an explicit ClickHouse SQL implementation instead. |

Create the target table with `create+replace` or `ddl` before its first `append`, `delete+insert`, `merge`, `time_interval`, or `truncate+insert` run. ClickHouse validates the chosen engine's key requirements. Declaring a primary key alone does not deduplicate rows during `create+replace`; deduplication depends on the chosen ClickHouse engine.

View materializations support only the default strategy, which creates or replaces the view. Table-only strategies, including all incremental strategies, are not supported for views.

> [!NOTE]
> ClickHouse statements are executed one at a time and are not wrapped in a transaction. Without `cluster`, `create+replace` is a single `CREATE OR REPLACE TABLE` statement, but a failed `delete+insert`, `merge`, `time_interval`, or `truncate+insert` run can leave the target table between steps. Use idempotent, date- or partition-bounded queries and rerun the asset to recover.

#### Native SQL table definitions

For native `clickhouse.sql` assets, use a top-level `clickhouse` block for table engine, sorting key, TTL, and settings. Partitioning stays in `materialization.partition_by`:

```bruin-sql
/* @bruin
name: analytics.events
type: clickhouse.sql
materialization:
  type: table
  strategy: create+replace
  partition_by: toYYYYMM(created_at)
clickhouse:
  engine: ReplacingMergeTree(version)
  order_by:
    - id
    - created_at
  ttl: created_at + INTERVAL 30 DAY
  settings:
    index_granularity: "8192"
columns:
  - name: id
    type: UInt64
    primary_key: true
  - name: created_at
    type: DateTime
  - name: version
    type: UInt64
@bruin */

SELECT id, created_at, version FROM raw.events
```

Bruin renders the table definition as:

```sql
CREATE OR REPLACE TABLE analytics.events
ENGINE = ReplacingMergeTree(version)
PARTITION BY (toYYYYMM(created_at))
PRIMARY KEY (id)
ORDER BY (id, created_at)
TTL created_at + INTERVAL 30 DAY
SETTINGS index_granularity = 8192
AS SELECT id, created_at, version FROM raw.events
```

- `engine` is a ClickHouse SQL expression, such as `MergeTree()`, `ReplacingMergeTree(version)`, or `SummingMergeTree()`. If omitted, Bruin omits the engine clause and ClickHouse chooses its default engine. An explicit engine can be used without key metadata: for example, `Memory()` must omit primary and sorting keys, while MergeTree-family engines require a primary or sorting key.
- `order_by` is a list of separate SQL expressions forming the sorting key. Use `order_by: ["tuple()"]` for an explicitly empty sorting key. It can be provided without any columns marked `primary_key: true`.
- `ttl` is the SQL TTL expression, without the `TTL` keyword.
- `settings` is a mapping of setting names to SQL values. Bruin sorts setting names for stable SQL output. Numeric values can be written as `index_granularity: "8192"`; string literals need SQL quotes, for example `storage_policy: "'default'"`.

When both a primary key and `order_by` are provided, the primary-key columns must be a prefix of the sorting key, in column declaration order. For example, primary key `(id)` can use sorting key `(id, created_at)`. This follows the [ClickHouse MergeTree key requirements](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree). When only primary-key columns are declared, Bruin continues to emit `PRIMARY KEY` without an explicit `ORDER BY`.

All of these clauses, including `partition_by`, apply when `create+replace`, the implicit table strategy, or `ddl` creates the target table. A full refresh uses the same options. `ddl` uses `CREATE TABLE IF NOT EXISTS`, so changing options does not alter an existing table. Normal `append`, `delete+insert`, `merge`, `time_interval`, and `truncate+insert` runs keep the existing target definition. Internal staging tables use MergeTree and do not inherit the target's options. Views and other asset types reject the `clickhouse` options.

Set shared values in `pipeline.yml` under `default.clickhouse`:

```yaml
default:
  clickhouse:
    engine: MergeTree()
    settings:
      index_granularity: "8192"
```

Only native `clickhouse.sql` table assets inherit `default.clickhouse`; views and other asset types do not. Omitted or empty fields inherit their defaults. Settings merge by name, with asset values taking precedence over defaults. Partitioning inherits through `default.materialization.partition_by`.

The `clickhouse` block applies only to native SQL assets. Ingestr destinations use `parameters.engine` with names such as `replacing_merge_tree` and `parameters.engine.<setting>` instead; those parameters do not affect native SQL DDL.

#### Self-hosted clusters

Set the connection's `cluster` to a ClickHouse cluster name to add `ON CLUSTER` to generated native SQL materialization DDL. Leave it unset for ClickHouse Cloud, standalone servers, and databases that already distribute DDL implicitly. There is no per-asset cluster override: select a different connection on an asset when it needs a different cluster or local execution.

```yaml
connections:
  clickhouse:
    - name: clickhouse-replicated
      host: clickhouse-replica-1.example.com
      port: 9000
      username: bruin
      password: "XXXXXXXXXX"
      database: analytics
      secure: 0
      cluster: analytics_cluster
```

Use an existing `Atomic` database on every node, with the same cluster configuration, a working ClickHouse Keeper or ZooKeeper service, and permissions to execute distributed DDL. For data refreshes, the configured host must belong to the named cluster, and that cluster must contain every replica of the target's single shard. Otherwise, dropping and recreating replicas can leave old data outside the cluster. Bruin does not provision the cluster, databases, replicas, or sharding. See [ClickHouse's distributed DDL requirements](https://clickhouse.com/docs/sql-reference/distributed-ddl).

| Operation | Supported topology and behavior |
| --- | --- |
| Views and `ddl` | Creates definitions on all nodes, including multi-shard clusters. `ddl` requires an explicit `clickhouse.engine`; it can create local or `Distributed` tables. Referenced objects must exist on every relevant node. |
| `create+replace`, implicit table strategy, and full refresh | Supports one shard with replicated tables. Requires an explicit `Replicated*MergeTree` engine. Drops the target on all nodes with `SYNC`, creates empty tables with `ON CLUSTER`, then inserts once through the configured host. |
| `append` | Inserts once. The target may be a local replicated table in a single shard, or a separately provisioned `Distributed` table routing writes across shards. |
| `truncate+insert` | Supports an existing local replicated table in a single shard. Runs `TRUNCATE TABLE ... ON CLUSTER ... SYNC`, then inserts once. |
| `time_interval` | Supports an existing local replicated table in a single shard. Uses `ALTER TABLE ... ON CLUSTER ... DELETE WHERE ... SETTINGS mutations_sync = 2`, then inserts once with `insert_deduplicate = 0`. |
| `delete+insert` and `merge` | Normal runs are rejected. Their staging-table and key-subquery operations require a consistent staged dataset on every replica; Bruin does not yet coordinate this. An unrestricted full refresh instead uses the cluster `create+replace` path. |

`INSERT` has no `ON CLUSTER` clause. Bruin assumes that its one insert reaches the intended data: local replicated tables replicate that write within one shard; a `Distributed` target handles sharding itself. For replicated shards behind a `Distributed` table, configure `internal_replication: true` in ClickHouse. The asset query runs through the connected host and must select the complete input dataset. Bruin does not run one insert per node or automatically convert a local source into a distributed query. See [ClickHouse's distributed writes](https://clickhouse.com/docs/engines/table-engines/special/distributed#writing-data).

Multi-shard refreshes of local tables, refreshes or mutations through `Distributed` targets, and data materializations into independent non-replicated tables are unsupported. An explicitly non-replicated engine is rejected for cluster `truncate+insert` and `time_interval`; omitting the engine for these strategies assumes the existing target is a replicated local table. Bruin does not inspect the deployed topology or existing engine. Append through a `Distributed` table is supported, but Bruin does not create or maintain the underlying local tables automatically.

Choose replication arguments explicitly, for example:

```yaml
clickhouse:
  engine: "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', version)"
  order_by: [id]
```

Bruin preserves the engine expression, including all arguments; it does not prepend replication parameters or convert engines. The Keeper path must be unique per table and shard and identical across that shard's replicas. The replica name must differ per replica, normally through the `{replica}` server macro. `ReplicatedMergeTree()` and variants with omitted replication arguments use the server's `default_replica_path` and `default_replica_name`; configure these consistently. See [replicated engine arguments](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication#replicatedmergetree-parameters).

For example, a cluster `create+replace` emits:

```sql
DROP TABLE IF EXISTS analytics.events ON CLUSTER `analytics_cluster` SYNC;
CREATE TABLE analytics.events ON CLUSTER `analytics_cluster`
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
ORDER BY (id)
EMPTY AS SELECT id FROM raw.events;
INSERT INTO analytics.events SELECT id FROM raw.events;
```

`EMPTY AS SELECT` infers the schema without inserting the source data on every node. It requires ClickHouse 22.7 or newer, and the schema query's sources must be accessible on every node. The synchronous drop allows recreation with a fixed Keeper path. This refresh is **not atomic**: readers can see a missing or empty table, and a failure can leave a partially rebuilt target. The source query must not read the target being dropped, truncated, or deleted from.

Bruin waits for distributed DDL completion with `distributed_ddl_output_mode = 'throw'` and a 180-second `distributed_ddl_task_timeout`. `TRUNCATE ... SYNC` waits across replicas; interval deletion uses synchronous [mutation processing](https://clickhouse.com/docs/sql-reference/statements/alter/delete). A DDL error or timeout stops subsequent statements, but a timeout does not cancel queued DDL: restore cluster health and check its completion before retrying. Inserts retain ClickHouse's replication and durability settings; completion does not guarantee immediate visibility on every replica.

The connection's `cluster` applies only to Bruin-generated native SQL materializations. Raw SQL scripts are executed as written, and seeds and ingestr assets do not gain cluster support from this field.

#### `delete+insert` example

On connections without `cluster`, use `delete+insert` when the query returns complete replacements for one or more incremental-key values. For example, this refreshes every `dt` returned by the query:

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

Running `bruin run --full-refresh` changes every ClickHouse table materialization except `ddl` to `create+replace`, unless the asset has `full_refresh_restricted: true` (or its `refresh_restricted` alias). This includes `time_interval`: it does **not** use interval-based deletion during a full refresh. Without `cluster`, the rebuild runs `CREATE OR REPLACE TABLE` with the configured table options and the same key requirements as `create+replace`. With `cluster`, it uses the drop, empty create, and single insert sequence described above and requires an explicit replicated engine. A `ddl` asset remains `CREATE TABLE IF NOT EXISTS` during a full refresh.

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
- an empty insert validates the staged result against the target table before any target rows are deleted
- rows in the target whose primary key matches a staged row are deleted
- the staged rows are inserted into the target
- the temporary table is dropped

This upserts rows by primary key: existing rows are replaced and new rows are added, while untouched rows remain. The optional `incremental_predicate` is appended to the delete condition to scope which target rows are considered for replacement.

The compatibility check catches errors such as a different number of staged and target columns before the delete. If a later statement fails, Bruin makes a best-effort attempt to drop the per-run staging table. The merge still consists of independent ClickHouse statements rather than a transaction, so an operational failure during the real insert can leave the matching target rows deleted; fix the failure and rerun the asset to restore them.

Unlike merge implementations that expose `source` and `target` aliases, ClickHouse's `DELETE` statement has no target alias. A ClickHouse `incremental_predicate` must therefore reference target columns without qualification, for example `event_date >= toDate('2026-07-01')` rather than `target.event_date >= toDate('2026-07-01')`.

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
