# AWS Athena

Bruin supports AWS Athena as a query engine, which means you can use Bruin to build tables and views in your data lake with Athena.

> [!WARNING]
> Bruin materializations will always create Iceberg tables on Athena. You can still write SQL scripts for legacy tables and not use [materialization](../assets/materialization.md) features.


## Connection
In order to have set up an Athena connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
connections:
    athena:
        - name: "connection_name"
          region: "us-west-2"
          database: "some_database" 
          access_key: "XXXXXXXX"
          secret_key: "YYYYYYYY"
          query_results_path: "s3://some-bucket/some-path" 
```

The field `database` is optional, if not provided, it will default to `default`.

> [!WARNING]
> The results of the materialization as well as any temporary tablesBruin needs to create will be stored at the location defined by `query_results_path`. This location must be writable and might be required to be empty at the beginning. 


## Athena Assets

### `athena.sql`
Runs a materialized Athena asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

### Examples
Create a view to aggregate website traffic data
```bruinsql
/* @bruin
name: website_traffic.view
type: athena.sql
materialization:
    type: view
@bruin */

select
    date,
    count(distinct user_id) as unique_visitors,
    sum(page_views) as total_page_views,
    avg(session_duration) as avg_session_duration
from raw_web_traffic
group by date;
```

Create a table to analyze daily sales performance:
```bruinsql
/* @bruin
name: daily_sales_analysis.view
type: athena.sql
materialization:
    type: table
@bruin */

select
    order_date,
    sum(total_amount) as total_sales,
    count(distinct order_id) as total_orders,
    avg(total_amount) as avg_order_value
from sales_data
group by order_date;
```

Bruin Athena assets support partitioning by one column only
```bruinsql
/* @bruin
name: daily_sales_analysis.view
type: athena.sql
materialization:
    type: table
    partition_by: order_date # <----------
@bruin */

select
    order_date,
    sum(total_amount) as total_sales,
    count(distinct order_id) as total_orders,
    avg(total_amount) as avg_order_value
from sales_data
group by order_date;
```
