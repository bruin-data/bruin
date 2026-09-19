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

## Sensors

Sensors are a special type of asset used to wait on external signals before downstream assets run. Fabric sensors run against the Warehouse SQL endpoint, so they work with both Microsoft Entra ID and SQL authentication.

Sensors are defined as YAML files ending in `.asset.yml`. Bruin runs sensors once by default; use the `--sensor-mode` flag on `bruin run` to switch to `wait` or `skip`.

### `fabric.sensor.table`

Checks whether a table exists in the Fabric Warehouse, poking until it appears.

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
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
- `timeout`: How long to wait before the sensor fails. Uses single-unit duration syntax (`s`, `m`, `h`, `d`, `ms`, `ns`), e.g. `1h` or `90m`. Defaults to `24h`. See [Sensor Timeout](/assets/sensor#timeout).

```yaml
name: analytics.upstream_events
type: fabric.sensor.table
parameters:
    table: raw.events
```

### `fabric.sensor.query`

Runs a query that returns a single value and pokes until that value is greater than zero.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
    timeout: duration (optional)
```

**Parameters**:

- `query`: A query returning exactly one row with one column. The sensor succeeds once that value is greater than zero, and keeps poking while it is zero or the query returns no rows. Booleans are accepted, with `true` counting as `1`. A query returning multiple rows or columns fails the asset rather than poking, so shape the query with `exists`, `count`, or a `case` expression as in the examples below.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).
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
