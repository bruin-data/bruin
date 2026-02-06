# Microsoft SQL Server

Bruin supports Microsoft SQL Server as a data platform.

> [!NOTE]
> We tend to use "MS SQL" interchangeably to refer to Microsoft SQL Server, apologies for any confusion.


## Connection
In order to set up a SQL Server connection in Bruin, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      mssql:
        - name: "connection_name"
          username: "mssql_user"
          password: "XXXXXXXXXX"
          host: "mssql_host.somedomain.com"
          port: 1433
          database: "dev"
          options: "encrypt=disable&TrustServerCertificate=true"  # optional
```

### Connection Parameters

- `name` (required): The name of the connection to be used in assets
- `username` (required): SQL Server username
- `password` (required): SQL Server password
- `host` (required): Hostname or IP address of the SQL Server
- `port` (optional): Port number (default: 1433)
- `database` (required): Database name to connect to
- `options` (optional): Additional connection string parameters

### Connection Options

The `options` field allows you to customize the connection behavior with additional parameters. If not specified, Bruin uses safe defaults suitable for local development and Docker environments.

#### Default Behavior (No Options)

When `options` is not specified, these defaults are applied:
- `TrustServerCertificate=true` - Trust self-signed certificates
- `encrypt=disable` - Disable encryption (suitable for local/Docker)
- `app name=Bruin CLI` - Application identifier

#### Common Use Cases

**Production with Full Encryption:**
```yaml
connections:
  mssql:
    - name: "mssql_prod"
      username: "prod_user"
      password: "SecurePassword"
      host: "production.database.azure.com"
      port: 1433
      database: "ProductionDB"
      options: "encrypt=true&TrustServerCertificate=false"
```

**Azure SQL Database:**
```yaml
connections:
  mssql:
    - name: "mssql_azure"
      username: "user@server"
      password: "password"
      host: "myserver.database.windows.net"
      port: 1433
      database: "mydb"
      options: "encrypt=true"
```

**Local Development (Explicit):**
```yaml
connections:
  mssql:
    - name: "mssql_local"
      username: "sa"
      password: "LocalPass123"
      host: "localhost"
      port: 1433
      database: "devdb"
      options: "encrypt=disable&TrustServerCertificate=true"
```

**Custom Connection Timeout:**
```yaml
connections:
  mssql:
    - name: "mssql_custom"
      username: "user"
      password: "password"
      host: "server.com"
      port: 1433
      database: "mydb"
      options: "connection timeout=30&encrypt=true"
```

#### Available Options

Common SQL Server connection string parameters:

| Parameter | Values | Description |
|-----------|--------|-------------|
| `encrypt` | `true`, `false`, `disable` | Enable/disable encryption |
| `TrustServerCertificate` | `true`, `false` | Trust server certificate |
| `connection timeout` | number | Connection timeout in seconds (default: 30) |
| `app name` | string | Application name identifier |
| `ApplicationIntent` | `ReadOnly`, `ReadWrite` | For Always On availability groups |
| `MultiSubnetFailover` | `true`, `false` | For failover scenarios |
| `packet size` | 4096-32767 | Network packet size |

For a complete list of available parameters, see the [go-mssqldb documentation](https://github.com/microsoft/go-mssqldb?tab=readme-ov-file#connection-parameters-and-dsn).


## SQL Server Assets

### `ms.sql`
Runs a materialized SQL Server asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Examples
Run an MS SQL script to generate sales report
```bruin-sql
/* @bruin
name: sales_report
type: ms.sql
@bruin */

with monthly_sales as (
    select
        product_id,
    year(order_date) as order_year,
    month(order_date) as order_month,
    sum(quantity) as total_quantity,
    sum(price) as total_sales
from sales.orders
group by product_id, year(order_date), month(order_date)
    )
select
    product_id,
    order_year,
    order_month,
    total_quantity,
    total_sales
from monthly_sales
order by order_year, order_month;
```

### `ms.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.

Checks if a table exists in MSSQL, runs by default every 30 seconds until this table is available.

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



### `ms.sensor.query`

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
type: ms.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: ms.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `ms.seed`
`ms.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your MSSQL database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the MSSQL database.

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:
```yaml
name: dashboard.hello
type: ms.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.


####  Examples: Load csv into a MSSQL database

The examples below show how to load a CSV into an MSSQL database.
```yaml
name: dashboard.hello
type: ms.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
