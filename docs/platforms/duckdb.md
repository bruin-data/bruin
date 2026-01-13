# Duck DB

DuckDB is an in-memory database designed to be fast and reliable.

Bruin supports using a local DuckDB database.

## Connection

```yaml
    connections:
      duckdb:
        - name: "connection_name"
          path: "/path/to/your/duckdb/database.db"
```

The field `path` is the only one you need and it can point to an existing database or the full path of the database that you want to create and where your queries would be materialized.

> [!WARNING]
> DuckDB does not allow concurrency between different processes, which means other clients should not be connected to the database while Bruin is running.


## Assets

DuckDB assets should use the type `duckdb.sql` and if you specify a connection it must be of the `duckdb` type.
For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


### Examples

Create a view with orders per country
```bruin-sql
/* @bruin
name: orders_per_country
type: duckdb.sql
materialization:
    type: view
@bruin */

SELECT COUNT(*) as orders, country
FROM events.orders
WHERE status = "paid"
GROUP BY country
```

Materialize new customers per region and append them to an existing table
```bruin-sql
/* @bruin
name: new_customers_per_region
type: duckdb.sql
materialization:
    type: table
    strategy: append
@bruin */

SELECT COUNT(*) as customers, region 
    WHERE created_at >= {{ start_date }} 
      AND created_at < {{ end_date }}
FROM events.customers
```

### `duckdb.sensor.query`

Checks if a query returns any results in DuckDB, runs every 5 minutes until this query returns any results.

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
type: duckdb.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: duckdb.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```


### `duckdb.seed`
`duckdb.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your DuckDB database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the DuckDB database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`: The path to the CSV file that will be loaded into the data platform. This can be a relative file path (relative to the asset definition file) or an HTTP/HTTPS URL to a publicly accessible CSV file.

> [!WARNING]
> When using a URL path, column validation is skipped during `bruin validate`. Column mismatches will be caught at runtime.


####  Examples: Load csv into a Duckdb database

The examples below show how to load a CSV into a DuckDB database.
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
