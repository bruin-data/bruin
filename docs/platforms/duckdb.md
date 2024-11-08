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
```sql
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
```sql
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

