# MotherDuck

MotherDuck is a cloud-native data warehouse built on DuckDB, combining DuckDB's performance with cloud scalability and collaboration features.

Bruin supports connecting to MotherDuck databases using your MotherDuck token.

## Connection

```yaml
    connections:
      motherduck:
        - name: "connection_name"
          token: "your_motherduck_token"
          database: "database_name"  # optional
```

**Parameters**:
- `token`: Your MotherDuck authentication token (required)
- `database`: The specific database to connect to in your MotherDuck account (optional)

If no database is specified, you'll connect to your default MotherDuck database.

> [!NOTE]
> You can obtain your MotherDuck token from the MotherDuck web interface. Keep this token secure and consider using environment variables in your configuration.

## Assets

MotherDuck assets use the same asset types as DuckDB since MotherDuck is built on DuckDB. You can use `duckdb.sql`, `duckdb.sensor.query`, and `duckdb.seed` asset types with MotherDuck connections.

For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

### Examples

Create a view with orders per country using MotherDuck:
```bruin-sql
/* @bruin
name: orders_per_country
type: duckdb.sql
connection: my_motherduck_connection
materialization:
    type: view
@bruin */

SELECT COUNT(*) as orders, country
FROM events.orders
WHERE status = "paid"
GROUP BY country
```

Materialize new customers per region and append them to an existing table:
```bruin-sql
/* @bruin
name: new_customers_per_region
type: duckdb.sql
connection: my_motherduck_connection
materialization:
    type: table
    strategy: append
@bruin */

SELECT COUNT(*) as customers, region 
FROM events.customers
    WHERE created_at >= {{ start_date }} 
      AND created_at < {{ end_date }}
```

### `duckdb.sensor.query`

Works identically with MotherDuck connections. Checks if a query returns any results in your MotherDuck database, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: duckdb.sensor.query
connection: my_motherduck_connection
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data is available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: duckdb.sensor.query
connection: my_motherduck_connection
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}")
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: duckdb.sensor.query
connection: my_motherduck_connection
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}")
```

### `duckdb.seed`

`duckdb.seed` assets work seamlessly with MotherDuck connections. This asset type is used to represent CSV files that contain data prepared outside of your pipeline that will be loaded into your MotherDuck database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed
connection: my_motherduck_connection

parameters:
    path: seed.csv
```

**Parameters**:
- `path`: The path parameter is the path to the CSV file that will be loaded into MotherDuck. The path is relative to the asset definition file.

#### Examples: Load CSV into MotherDuck database

The example below shows how to load a CSV into a MotherDuck database.
```yaml
name: dashboard.hello
type: duckdb.seed
connection: my_motherduck_connection

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

## Key Differences from Local DuckDB

- **Cloud-native**: Your data is stored in MotherDuck's cloud infrastructure
- **Collaboration**: Multiple users can work with the same databases simultaneously
- **Scalability**: Automatic scaling based on workload requirements
- **Authentication**: Requires a MotherDuck token for access
- **Persistence**: Data persists across sessions without managing local files

## Migration from Local DuckDB

To migrate from local DuckDB to MotherDuck:

1. Update your connection configuration to use `motherduck` instead of `duckdb`
2. Add your MotherDuck token and database information
3. Your existing `duckdb.sql`, `duckdb.sensor.query`, and `duckdb.seed` assets will work without modification
4. Update asset definitions to reference your MotherDuck connection

All DuckDB SQL syntax and functions are fully supported in MotherDuck.