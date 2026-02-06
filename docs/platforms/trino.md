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
Runs a materialized Trino asset or a Trino script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page. For information about materialization strategies, see the [Materialization](../assets/materialization.md) page.

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: hive.events.install
type: trino.sql
materialization:
    type: table
@bruin */

SELECT user_id, event_name, ts
FROM hive.analytics.events
WHERE event_name = 'install'
```

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

### `trino.sensor.query`

Checks if a query returns any results in Trino, runs every 30 seconds until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
```

**Parameters:**
- `query`: Query you expect to return any results
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

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


## Lakehouse Support (Planned)

> [!WARNING]
> Trino lakehouse support is not yet available. This section documents the planned configuration.

### Planned Support

| Catalog \ Storage | S3 |
|-------------------|----|
| Glue | Planned |
| Nessie | Planned |