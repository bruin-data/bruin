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
          access_key_id: "XXXXXXXX"
          secret_access_key: "YYYYYYYY"
          query_results_path: "s3://some-bucket/some-path"
          session_token: "ZZZZZZZ" # optional
          profile: "some_profile" # optional
```

You have two ways to set credentials:
- You can put your `access_key_id` and `secret_access_key`, as well as an optional `session_token` here directly.
- Alternatively, you can put your `profile` here, and if you have your local AWS credentials in `~/.aws` configured, Bruin will use them.

The field `database` is optional, if not provided, it will default to `default`.

> [!WARNING]
> The results of the materialization as well as any temporary tables Bruin needs to create will be stored at the location defined by `query_results_path`. This location must be writable and might be required to be empty at the beginning. 


## Athena Assets

### `athena.sql`
Runs a materialized Athena asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

### Examples
Create a view to aggregate website traffic data
```bruin-sql
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
```bruin-sql
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
```bruin-sql
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


### `athena.seed`
`athena.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Athena database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Athena database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: athena.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Athena database

The examples below show how to load a CSV into an Athena database.
```yaml
name: dashboard.hello
type: athena.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
