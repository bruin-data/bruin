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

Checks if a table exists in Postgres, runs by default every 30 seconds until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
    poke_interval: int (optional)
```

**Parameters**:

- `table`: `schema_id.table_id` or (for default schema `public`) `table_id` format.
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

### `pg.sensor.query`

Checks if a query returns any results in Postgres, runs by default every 30 seconds until this query returns any results.

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

You can define seed assets in a file ending with `.asset.yml` or `.asset.yaml`:

```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

**Parameters**:

- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.

#### Examples: Load csv into a Postgres database

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

## CDC (Change Data Capture)

Bruin supports PostgreSQL CDC via the `ingestr` asset type. CDC uses PostgreSQL's [logical replication](https://www.postgresql.org/docs/current/logical-replication.html) to capture row-level changes (inserts, updates, deletes) and replicate them to a destination.

### Prerequisites

Your source PostgreSQL database must be configured for logical replication:

- `wal_level` set to `logical`
- A [publication](https://www.postgresql.org/docs/current/sql-createpublication.html) created for the tables you want to replicate
- A [replication slot](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS) allocated for the CDC consumer

Refer to the [PostgreSQL logical replication documentation](https://www.postgresql.org/docs/current/logical-replication.html) for detailed setup instructions.

### Parameters

CDC is enabled by setting `cdc: "true"` on an `ingestr` asset with a PostgreSQL source connection.

| Parameter | Required | Description |
|-----------|----------|-------------|
| `cdc` | Yes | Set to `"true"` to enable CDC mode |
| `cdc_mode` | No | `"stream"` for real-time streaming or `"batch"` for batch replication |
| `cdc_publication` | No | Name of the PostgreSQL publication to use |
| `cdc_slot` | No | Name of the PostgreSQL replication slot to use |
| `source_table` | Yes | Source table in `schema.table` format, or `"*"` to replicate all tables in the publication |
| `incremental_strategy` | No | Defaults to `"merge"` when CDC is enabled; can be overridden to `"append"` |

> [!NOTE]
> When CDC is enabled, primary key columns do not need to be specified in the asset definition — they are determined automatically from the source table.

### Examples

#### Basic CDC replication
```yaml
name: public.users
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.users
  destination: bigquery
  cdc: "true"
```

#### CDC with explicit publication and slot
```yaml
name: public.users
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.users
  destination: bigquery
  cdc: "true"
  cdc_publication: my_publication
  cdc_slot: my_slot
```

#### CDC with stream mode
```yaml
name: public.orders
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: public.orders
  destination: bigquery
  cdc: "true"
  cdc_mode: stream
```

#### Wildcard CDC — replicate all tables
When `source_table` is set to `"*"`, all tables in the publication are replicated to the destination.

```yaml
name: public.all_tables
type: ingestr
connection: bigquery

parameters:
  source_connection: my_pg
  source_table: "*"
  destination: bigquery
  cdc: "true"
```
