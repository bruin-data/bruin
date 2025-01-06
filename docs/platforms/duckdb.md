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


### `duckdb.seed`
`duckdb.seed` are a special type of assets that are used to represent are CSV-files that contain data that is prepared outside of your pipeline that will be loaded into your duckdb database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the duckdb database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file. If the path is not provided, the asset name will be used to find the CSV file in the same directory as the asset definition file.


####  Examples: Load csv into a Duckdb database

The examples below show how load a csv into a duckdb database.
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
