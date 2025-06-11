# Snowflake Assets

Bruin supports Snowflake as a data platform. 

## Connection
In order to set up a Snowflake connection, you need to add a configuration item to `connections` in the `.bruin.yml` file.

There's 2 different ways to fill it in

```yaml
    connections:
      snowflake:
        - name: "connection_name"
          username: "sfuser"
          password: "XXXXXXXXXX"
          account: "AAAAAAA-AA00000"
          database: "dev"
          schema: "schema_name" # optional
          warehouse: "warehouse_name" # optional
          role: "data_analyst" # optional
          region: "eu-west1" # optional
          # specify either private_key or private_key_path for key-based authentication
          private_key_path: "path/to/private_key" # optional
          # private_key: |
          #   -----BEGIN PRIVATE KEY-----
          #   ...
          #   -----END PRIVATE KEY-----
```

Where account is the identifier that you can copy here:

![Snowflake Account](/snowflake.png)


### Key-based Authentication

Snowflake currently supports both password-based authentication as well as key-based authentication. For key-based auth you can either embed the key directly using the `private_key` option or reference it with `private_key_path`. See [this guide](https://select.dev/docs/snowflake-developer-guide/snowflake-key-pair) to create a key-pair if you haven't done that before.


## Snowflake Assets

### `sf.sql`
Runs a materialized Snowflake asset or a Snowflake script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: sf.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a Snowflake script
```bruin-sql
/* @bruin
name: events.install
type: sf.sql
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

### `sf.sensor.query`


Checks if a query returns any results in Snowflake, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters:**
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table
Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: sf.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table
Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: sf.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `sf.seed`
`sf.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Snowflake database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Snowflake database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: sf.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Snowflake database

The examples below show how to load a CSV into a Snowflake database.
```yaml
name: dashboard.hello
type: sf.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
