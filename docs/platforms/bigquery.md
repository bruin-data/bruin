# Google BigQuery

Google BigQuery is a fully-managed, serverless data platform that enables super-fast SQL queries using the processing power of Google's infrastructure.

Bruin supports BigQuery as a data platform.

## Connection

Google BigQuery requires a Google Cloud Platform connection, which can be added as a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
    connections:
      google_cloud_platform:
        - name: "connection_name"
          project_id: "project-id"
          
          # you can either specify a path to the service account file
          service_account_file: "path/to/file.json"
          
          # or you can specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```

## BigQuery Assets

### `bq.sql`
Runs a materialized BigQuery asset or a BigQuery script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: bq.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a BigQuery script
```bruin-sql
/* @bruin
name: events.install
type: bq.sql
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


### `bq.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.


Checks if a table exists in BigQuery, runs every 5 minutes until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
```
**Parameters**:
- `table`: `project-id.dataset_id.table_id` format, requires all of the identifiers as a full name.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

#### Examples
```yaml
# Google Analytics Events that checks if the recent date table is available
name: analytics_123456789.events
type: bq.sensor.table
parameters:
    table: "your-project-id.analytics_123456789.events_{{ end_date | date_format('%Y%m%d') }}"
```

### `bq.sensor.query`

Checks if a query returns any results in BigQuery, runs every 5 minutes until this query returns any results.

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
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `bq.seed`
`bq.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your BigQuery database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the BigQuery database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: bq.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a BigQuery database

The examples below show how to load a CSV into a BigQuery database.
```yaml
name: dashboard.hello
type: bq.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
