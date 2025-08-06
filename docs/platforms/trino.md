# Trino
Bruin supports Trino as a distributed SQL query engine.

## Connection
In order to set up a Trino connection, you need to add a configuration item to `connections` in the `.bruin.yml` file.

```yaml
    connections:
      trino:
        - name: "connection_name"
          username: "trino_user"
          password: "XXXXXXXXXX"  # Optional  
          host: "trino-coordinator.example.com"
          port: 8080
          catalog: "default" # Optional 
          schema: "schema_name" # Optional 
```

## Trino Assets

### `trino.sql`
Runs a Trino script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Run a Trino script
```bruin-sql
/* @bruin
name: hive.events.install
type: trino.sql
@bruin */

CREATE TABLE IF NOT EXISTS hive.events.install AS
SELECT user_id, event_name, ts
FROM hive.analytics.events
WHERE event_name = 'install'
```

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: iceberg.analytics.user_summary
type: trino.sql
materialization:
    type: table
    strategy: create+replace
    partition_by: dt
@bruin */

SELECT 
    user_id,
    COUNT(*) as event_count,
    DATE(ts) as dt
FROM iceberg.analytics.events
WHERE ts >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY user_id, DATE(ts)
```

#### Example: Create a view
```bruin-sql
/* @bruin
name: iceberg.analytics.active_users
type: trino.sql
materialization:
    type: view
@bruin */

SELECT DISTINCT user_id
FROM iceberg.analytics.events
WHERE ts >= CURRENT_DATE - INTERVAL '7' DAY
```

#### Example: Append data to existing table
```bruin-sql
/* @bruin
name: iceberg.analytics.daily_events
type: trino.sql
materialization:
    type: table
    strategy: append
@bruin */

SELECT 
    user_id,
    event_name,
    ts,
    DATE(ts) as dt
FROM iceberg.analytics.events
WHERE DATE(ts) = CURRENT_DATE
```

#### Example: Incremental update with delete+insert
```bruin-sql
/* @bruin
name: iceberg.analytics.user_profiles
type: trino.sql
materialization:
    type: table
    strategy: delete+insert
    incremental_key: user_id
@bruin */

SELECT 
    user_id,
    first_name,
    last_name,
    email,
    updated_at
FROM iceberg.staging.users
WHERE updated_at >= CURRENT_DATE - INTERVAL '1' DAY
```

#### Example: Time-based incremental loading
```bruin-sql
/* @bruin
name: iceberg.analytics.hourly_metrics
type: trino.sql
materialization:
    type: table
    strategy: time_interval
    incremental_key: hour
    time_granularity: timestamp
@bruin */

SELECT 
    DATE_TRUNC('hour', ts) as hour,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM iceberg.analytics.events
WHERE ts >= '{{ start_timestamp }}' 
  AND ts < '{{ end_timestamp }}'
GROUP BY DATE_TRUNC('hour', ts)
```

#### Example: Create table with DDL strategy
```bruin-sql
/* @bruin
name: iceberg.analytics.product_catalog
type: trino.sql
materialization:
    type: table
    strategy: ddl
    partition_by: category
columns:
  - name: product_id
    type: VARCHAR
    description: "Unique product identifier"
  - name: product_name
    type: VARCHAR
    description: "Product display name"
  - name: category
    type: VARCHAR
    description: "Product category"
  - name: price
    type: DOUBLE
    description: "Product price"
@bruin */

-- This query will be ignored when using DDL strategy
-- The table structure is defined in the columns section above
SELECT 1 as dummy
```

### `trino.sensor.query`

Checks if a query returns any results in Trino, runs every 30 seconds until this query returns any results.

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
type: trino.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = '{{ end_date }}')
```

#### Example: Streaming upstream table
Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: trino.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > '{{ end_timestamp }}')
```

## Materialization Support

Trino supports the following materialization strategies:

### Table Materializations

- **`create+replace`**: Overwrites the existing table with the new version. Creates tables in PARQUET format by default.
- **`append`**: Only appends new data to the table, never overwrites existing data.
- **`delete+insert`**: Incrementally updates the table by deleting records based on the `incremental_key` and inserting new ones.
- **`time_interval`**: Incrementally loads time-based data within specific time windows. Requires `incremental_key` and `time_granularity` (either `date` or `timestamp`).
- **`ddl`**: Creates a new table using DDL (Data Definition Language) statements. Requires the `columns` field to be defined.

### View Materializations

- **`none`**: Creates or replaces a view with the query results.

### Partitioning

Trino supports table partitioning using the `partition_by` parameter:

```yaml
materialization:
    type: table
    strategy: create+replace
    partition_by: dt
```

### Unsupported Strategies

The following materialization strategies are not yet supported in Trino:
- `merge`: Merge strategy is not implemented
- `scd2_by_column`: SCD2 by column strategy is not implemented  
- `scd2_by_time`: SCD2 by time strategy is not implemented 
