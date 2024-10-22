# Duck DB

Bruin supports using a local DuckDB database as a connection. This could come very handy when you want to test your queries locally before running them on a production database.

```yaml
    connections:
      duckdb:
        - name: "connection_name"
          path: "/path/to/your/duckdb/database.db"
```

The field `path` is the only one you need and it can point to an existing database or the full path of the database that you want to create and where your queries would be materialized.

It's important to note that other clients should not be connected to the database while Bruin is running since duck db does not allow concurrency between different processes.


## Assets

Duck DB assets should use the type `duckdb.sql` and if you specify a connection it must be of the `duckdb` type.
For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


### Examples

Create a view with orders per country
```sql
/* @bruin
name: orders_per_country
type: duck.sql
materialization:
    type: view
@bruin */

select COUNT(*) as orders, country
from events.orders
where status = "paid"
group by country
```

Materialize new customers per region and append them to an existing table
```sql
/* @bruin
name: new_customers_per_region
type: duck.sql
materialization:
    type: table
    strategy: append
@bruin */

select COUNT(*) as customers, region WHERE created_at >= {{ start_date }} AND created_at < {{ end_date }}
from events.customers
```

