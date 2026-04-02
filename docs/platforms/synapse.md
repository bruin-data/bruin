# Microsoft Azure Synapse

Bruin supports Azure Synapse as a data platform, which means you can use it to build data pipelines on Synapse.

> [!WARNING]
> I'll be honest with you: Synapse is the least used platform in the list, so there might be rough edges. If you run into any issues, please let us know by opening an issue on [GitHub](https://github.com/bruin-data/bruin/issues).

## Connection

Synapse connection is configured the same way as Microsoft SQL Server connection, check [SQL Server connection](mssql.md#connection) for more details.

```yaml
    connections:
      synapse:
        - name: "connection_name"
          username: "synapse_user"
          password: "XXXXXXXXXX"
          host: "synapse_host.sql.azuresynapse.net"
          port: 1433
          database: "dev"
          options: "encrypt=disable&TrustServerCertificate=true"  # optional
```

## Synapse Assets

### `synapse.sql`

Runs a materialized Synapse asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a view using view materialization

```bruin-sql
/* @bruin
name: customer_data.view
type: synapse.sql
materialization:
    type: view
@bruin */

select customer_id, first_name, last_name, email, country
from sales.customers
where active = 1
```

#### Example: Run a Synapse SQL script

```bruin-sql
/* @bruin
name: orders_summary
type: synapse.sql
@bruin */

create table temp_orders as
select
    order_id,
    order_date,
    customer_id,
    sum(quantity) as total_quantity,
    sum(price) as total_price
from sales.orders
group by order_id, order_date, customer_id;

create or replace view orders_summary as
select
    customer_id,
    count(order_id) as total_orders,
    sum(total_quantity) as total_quantity,
    sum(total_price) as total_price
from temp_orders
group by customer_id;
```

### `synapse.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.

Checks if a table exists in Synapse, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
```

**Parameters**:

- `table`: `schema_id.table_id` or (for default schema `dbo`) `table_id` format.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

### `synapse.sensor.query`

Checks if a query returns any results in SQL Server, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
```

**Parameters**:

- `query`: Query you expect to return any results
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.

```yaml
name: analytics_123456789.events
type: synapse.sensor.query
parameters:
    query: select case when exists(select 1 from upstream_table where dt = '{{ end_date }}') then 1 else 0 end
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.

```yaml
name: analytics_123456789.events
type: synapse.sensor.query
parameters:
    query: select case when exists(select 1 from upstream_table where inserted_at > '{{ end_timestamp }}') then 1 else 0 end
```

### `synapse.seed`

`synapse.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Synapse database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Synapse database.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

```yaml
name: dashboard.hello
type: synapse.seed

parameters:
    path: seed.csv
```

**Parameters**:

- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.

#### Examples: Load csv into a Synapse database

The examples below show how to load a CSV into a Synapse database.

```yaml
name: dashboard.hello
type: synapse.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

### `synapse.source`

Defines Synapse source assets for documenting existing tables and views in your Synapse database. These assets are no-op (they don't execute), but are useful for:

- Documenting existing Synapse tables and views
- Adding column descriptions and metadata
- Establishing lineage relationships
- Query preview functionality in the VSCode extension

#### Example: Document an existing Synapse table

```yaml
name: dbo.inventory
type: synapse.source
description: "Current inventory levels across all warehouses"
connection: synapse-default

tags:
  - inventory
  - logistics
domains:
  - supply-chain

meta:
  business_owner: "Logistics Team"
  data_steward: "warehouse-ops@company.com"
  refresh_frequency: "daily"

depends:
  - dbo.products

columns:
  - name: item_id
    type: "INT"
    description: "Unique identifier for each inventory item"
  - name: product_name
    type: "NVARCHAR(300)"
    description: "Name of the product"
  - name: quantity
    type: "INT"
    description: "Current quantity in stock"
  - name: last_updated
    type: "DATETIME2"
    description: "Timestamp of the last inventory update"
  - name: warehouse_code
    type: "VARCHAR(20)"
    description: "Code identifying the warehouse location"
```
