# PostgreSQL

Bruin supports PostgreSQL as a data platform.

## Connection
In order to set up a PostgreSQL connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      postgres:
        - name: "connection_name"
          username: "pguser"
          password: "XXXXXXXXXX"
          host: "pghost.somedomain.com"
          port: 5432
          database: "dev"
          ssl_mode: "allow" # optional
          schema: "schema_name" # optional
          pool_max_conns: 5 # optional
```

> [!NOTE]
> `ssl_mode` should be one of the modes describe in the [documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION).

## PostgreSQL Assets

### `pg.sql`
Runs a materialized Postgres asset or an sql script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: pg.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a Postgres script
```bruin-sql
/* @bruin
name: events.install
type: pg.sql
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

### `pg.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.


Checks if a table exists in BigQuery, runs every 5 minutes until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
```
**Parameters**:
- `table`: `schema_id.table_id` or (for default schema `public`) `table_id` format.


### `pg.sensor.query`

Checks if a query returns any results in Postgres, runs every 5 minutes until this query returns any results.

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
type: pg.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: pg.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `pg.seed`
`pg.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your PostgreSQL database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the PostgreSQL database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Postgres database

The examples below show how to load a CSV into a PostgreSQL database.
```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
