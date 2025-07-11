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
```


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

### `ms.sensor.query`

Checks if a query returns any results in SQL Server, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

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

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: ms.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


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
