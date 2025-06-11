# Databricks

Databricks is a unified data analytics platform that provides a collaborative environment for data scientists, data engineers, and business analysts. It is built on top of Apache Spark, which makes it easy to scale and process big data workloads.

Bruin supports Databricks as a data platform.

## Connection

In order to work with Databricks you can add as a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
    connections:
      databricks:
        - name: "connection_name"
          token: "your-databricks-token"
          path: "your-databricks-endpoint-path"
          host: "your-databricks-host"
          port: "your-databricks-port"
          catalog: "your-databricks-catalog"
          schema: "your-databricks-schema"
```

## Databricks Assets

### `databricks.sql`
Runs a materialized Databricks asset or a Databricks SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: databricks.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a script
```bruin-sql
/* @bruin
name: events.install
type: databricks.sql
@bruin */

create temp table first_installs as
select 
    user_id, 
    min(ts) as install_ts,
    min_by(platform, ts) as platform,
    min_by(country, ts) as country
from analytics.events
where event_name = "install"
group by 1;

create or replace table events.install
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel,
from first_installs as i
join marketing.attribution as a
    using(user_id)
```

### `databricks.sensor.query`

Checks if a query returns any results in Databricks, runs every 5 minutes until this query returns any results.

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
type: databricks.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: databricks.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```


### `databricks.seed`
`databricks.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Databricks database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Databricks database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: databricks.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Databricks database

The examples below show how to load a CSV into a Databricks database.
```yaml
name: dashboard.hello
type: databricks.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
