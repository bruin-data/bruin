# Microsoft Fabric

Bruin supports Microsoft Fabric Warehouse through the SQL endpoint (TDS protocol) and Fabric Lakehouses through the Fabric Livy API and Spark SQL. Both execution engines can share one Fabric connection and its Microsoft Entra credentials.

## Connection configuration

Add a Fabric connection under `fabric`. The optional `lakehouse` block enables Spark SQL assets without requiring a second `spark` connection:

```yaml
# .bruin.yml
environments:
  default:
    connections:
      fabric:
        - name: fabric-default
          host: sql-endpoint-guid.datawarehouse.fabric.microsoft.com
          port: 1433
          database: your_warehouse
          use_azure_default_credential: true
          lakehouse:
            workspace_id: "<workspace GUID>"
            lakehouse_id: "<lakehouse GUID>"
          # options: "encrypt=true&TrustServerCertificate=false" # optional
```

`host`, `port`, and `database` configure the Warehouse SQL endpoint. They are required on every Fabric connection, even one used only for `fabric.spark_sql` assets, so point them at your workspace's SQL analytics endpoint. `lakehouse.workspace_id` and `lakehouse.lakehouse_id` identify the Lakehouse used by `fabric.spark_sql`; authentication is inherited from the parent Fabric connection. The IDs are visible in the Fabric Lakehouse URL:

```text
https://app.fabric.microsoft.com/groups/<workspace_id>/lakehouses/<lakehouse_id>
```

### Azure AD (DefaultAzureCredential)

If `use_azure_default_credential: true` is set, the connector uses Azure's DefaultAzureCredential chain. You can authenticate locally with Azure CLI (`az login`).

Configure only one Microsoft Entra authentication method per connection. For backward compatibility, Warehouse and Spark use the default credential when both methods are configured, while ingestr continues to use the service principal.

### Service principal (client secret)

```yaml
environments:
  default:
    connections:
      fabric:
        - name: fabric-sp
          host: sql-endpoint-guid.datawarehouse.fabric.microsoft.com
          port: 1433
          database: your_warehouse
          client_id: "<app id>"
          client_secret: "<secret>"
          tenant_id: "<tenant id>"
```

### SQL authentication

```yaml
environments:
  default:
    connections:
      fabric:
        - name: fabric-sql
          host: sql-endpoint-guid.datawarehouse.fabric.microsoft.com
          port: 1433
          database: your_warehouse
          username: "<username>"
          password: "<password>"
```

> [!NOTE]
> SQL authentication is only available for Warehouse assets (`fabric.sql`, `fabric.seed`, sensors). Using `fabric.spark_sql` or Fabric as an [ingestr](#using-fabric-with-ingestr) source or destination requires Microsoft Entra ID authentication — username/password connections are rejected.

## Asset types

- `fabric.sql`
- `fabric.source`
- `fabric.spark_sql`
- `fabric.seed`
- `fabric.sensor.query`
- `fabric.sensor.table`

## Example asset

```sql
/* @bruin
name: my_schema.my_table
type: fabric.sql
materialization:
  type: table
  strategy: delete+insert
columns:
  - name: id
    type: int
    primary_key: true
  - name: name
    type: varchar(100)
  - name: updated_at
    type: datetime2
@bruin */

SELECT
    id,
    name,
    CAST(GETDATE() AS DATETIME2(6)) as updated_at
FROM source_table
WHERE modified_date > '{{ start_date }}'
```

## Merge

By default, `merge` on Fabric stages the query in a `<table>__bruin_tmp` table, deletes the target rows whose primary key is in it, and inserts every staged row. Matched rows are fully replaced, `update_on_merge` and `merge_sql` are ignored, and `incremental_predicate` is rejected.

Set `parameters.fabric_native_merge: true` to run a native T-SQL `MERGE` instead:

```sql
/* @bruin
name: sales.orders
type: fabric.sql
materialization:
  type: table
  strategy: merge
parameters:
  fabric_native_merge: true
columns:
  - name: order_id
    type: int
    primary_key: true
  - name: status
    type: varchar(20)
    merge_sql: CASE WHEN source.updated_at > target.updated_at THEN source.status ELSE target.status END
  - name: updated_at
    type: datetime2(6)
    merge_sql: CASE WHEN source.updated_at > target.updated_at THEN source.updated_at ELSE target.updated_at END
  - name: created_at
    type: datetime2(6)
@bruin */

WITH latest AS (
    SELECT order_id, status, updated_at, created_at FROM raw.orders
)
SELECT order_id, status, updated_at, created_at FROM latest
```

Bruin still stages the query with `CREATE TABLE ... AS`, because Fabric rejects a CTE inside `MERGE ... USING (...)`. The staging table gets a unique name on each run, so parallel backfill partitions don't collide. Bruin then merges the staged rows on the primary key and drops the staging table:

```sql
MERGE INTO [sales].[orders] target
USING [sales].[orders__bruin_tmp_<run>] source
ON target.[order_id] = source.[order_id]
WHEN MATCHED THEN UPDATE SET
  target.[status] = CASE WHEN source.updated_at > target.updated_at THEN source.status ELSE target.status END,
  target.[updated_at] = CASE WHEN source.updated_at > target.updated_at THEN source.updated_at ELSE target.updated_at END
WHEN NOT MATCHED BY TARGET THEN INSERT ([order_id], [status], [updated_at], [created_at])
  VALUES (source.[order_id], source.[status], source.[updated_at], source.[created_at]);
```

With `fabric_native_merge`:

- Only columns with `update_on_merge: true` or `merge_sql` change on a match. Other columns, such as `created_at` above, keep their existing values. Without any such column, matched rows are left untouched and only new keys are inserted.
- New keys insert the declared `columns`, so every column you want written must be listed.
- `incremental_predicate` is added to `WHEN MATCHED`, for example `WHEN MATCHED AND (source.updated_at > target.updated_at)`. It needs at least one `update_on_merge` or `merge_sql` column.
- The run fails before the `MERGE` when the query returns the same primary key more than once, with an error that names the key columns. The default delete+insert mode inserts every duplicate instead.

> [!WARNING]
> Turning on `fabric_native_merge` for an existing asset changes what happens to matched rows. A non-key column without `update_on_merge` or `merge_sql` stops being updated. Add `update_on_merge: true` to every column that should keep updating, or `merge_sql` for a conditional update, before you turn it on.

To turn it on for every asset in a pipeline, set it under `default.parameters` in `pipeline.yml`.

### First run of a `refresh_restricted` asset

A full refresh never runs for an asset with `refresh_restricted: true`, so nothing would create its table. With `fabric_native_merge`, such an asset creates a missing target from the declared `columns` before its query runs, as the `ddl` strategy does. This needs a `type` on every column. Fabric does not allow key constraints in `CREATE TABLE`, so no `PRIMARY KEY` is declared on the new table.

Because the table exists before the query runs, a query that reads its own target, for example to find the highest loaded ID, sees an empty table on the first run. `MERGE` inserts by column name, so the query can return the columns in a different order than `columns` declares them.

`append`, `delete+insert`, and `merge` without `fabric_native_merge` don't create a missing target. They copy rows by position, so a table built from `columns` would put values in the wrong column whenever the query orders them differently. Before you set `refresh_restricted: true`, create the table with one `bruin run --full-refresh` run, which builds it from the query in the query's column order.

## Source assets

Use `fabric.source` to document an existing Fabric Warehouse table or view for metadata and lineage without executing it.

```yaml
name: dbo.customers
type: fabric.source
description: Customer data managed outside Bruin
columns:
  - name: customer_id
    type: BIGINT
  - name: full_name
    type: VARCHAR(200)
```

## Sensors

Sensors are a special type of asset used to wait on external signals before downstream assets run. Fabric sensors run against the Warehouse SQL endpoint, so they work with both Microsoft Entra ID and SQL authentication.

Sensors are defined as YAML files ending in `.asset.yml`.

The `--sensor-mode` flag on `bruin run` controls what happens when the signal is not there yet. The default, `once`, checks a single time and fails the asset if the condition is unmet. `wait` re-checks every `poke_interval` until the condition holds or `timeout` elapses, and `skip` bypasses the sensor entirely. The `poke_interval` parameter therefore only takes effect under `--sensor-mode wait`.

### `fabric.sensor.table`

Checks whether a table exists in the Fabric Warehouse.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `table`: `database.schema.table`, `schema.table`, or `table` format. A bare `table` resolves against the default `dbo` schema.
- `poke_interval`: The interval between retries in seconds (default 30 seconds). Only used under `--sensor-mode wait`.
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

```yaml
name: analytics.upstream_events
type: fabric.sensor.table
parameters:
    table: raw.events
```

### `fabric.sensor.query`

Runs a query that returns a single value and succeeds when that value is greater than zero.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `query`: A query returning exactly one row with one column. The sensor is satisfied when that value is greater than zero; zero or no rows means the condition is unmet. Booleans are accepted, with `true` counting as `1`. A query returning multiple rows or columns fails the asset outright, so shape the query with `exists`, `count`, or a `case` expression as in the examples below.
- `poke_interval`: The interval between retries in seconds (default 30 seconds). Only used under `--sensor-mode wait`.
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

#### Example: Partitioned upstream table

Checks whether data is available in the upstream table for the run's end date.

```yaml
name: analytics.events
type: fabric.sensor.query
parameters:
    query: select case when exists(select 1 from upstream_table where dt = '{{ end_date }}') then 1 else 0 end
```

#### Example: Streaming upstream table

Checks whether any data landed after the end timestamp, assuming older data is never appended.

```yaml
name: analytics.events
type: fabric.sensor.query
parameters:
    query: select case when exists(select 1 from upstream_table where inserted_at > '{{ end_timestamp }}') then 1 else 0 end
```

## Spark SQL on a Lakehouse

Use `fabric.spark_sql` to execute Spark SQL through the [Fabric Livy API](https://learn.microsoft.com/fabric/data-engineering/api-livy-overview). It uses the same Spark SQL execution, checks, Jinja functions, and materialization machinery as `spark.sql`, while resolving the named connection from the `fabric` section.

```sql
/* @bruin
name: analytics.daily_events
type: fabric.spark_sql
connection: fabric-default
materialization:
  type: table
@bruin */

SELECT event_date, COUNT(*) AS event_count
FROM raw.events
GROUP BY event_date
```

Fabric Spark SQL authenticates with Microsoft Entra ID using the parent connection's credentials. When using a service principal, all three of `client_id`, `client_secret`, and `tenant_id` must be set. The authenticated identity must have permission to execute jobs on the Lakehouse.

## Using Fabric with Ingestr

A Fabric connection can be used as both a **source** and a **destination** for [ingestr](/ingestion/overview) assets, letting you move data between Fabric and any other supported platform.

> [!IMPORTANT]
> Fabric Warehouse only supports Microsoft Entra ID authentication for ingestr. Use a connection configured with `use_azure_default_credential: true` or a service principal (`client_id` / `client_secret` / `tenant_id`); username/password (SQL auth) connections cannot be used as an ingestr source or destination.
>
> Fabric ingestion requires ingestr `1.0.5` or newer, which is the default. No extra configuration is needed unless you have pinned an older release with `parameters.version`.

### Fabric as a destination

Load data from any source into a Fabric Warehouse by setting `destination: fabric`:

```yaml
name: raw.customers
type: ingestr
parameters:
  source_connection: my-shopify
  source_table: customers
  destination: fabric
```

### Fabric as a source

Point `source_connection` at a Fabric connection to load Fabric tables into another platform:

```yaml
name: raw.orders
type: ingestr
parameters:
  source_connection: fabric-default
  source_table: dbo.orders
  destination: bigquery
```
