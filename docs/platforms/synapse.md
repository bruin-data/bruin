# Microsoft Azure Synapse

Bruin supports Azure Synapse as a data platform, which means you can use it to build data pipelines on Synapse.

> [!WARNING]
> I'll be honest with you: Synapse is the least used platform in the list, so there might be rough edges. If you run into any issues, please let us know by opening an issue on [GitHub](https://github.com/bruin-data/bruin/issues).

## Connection
Synapse connection is configured the same way as Microsoft SQL Server connection, check [SQL Server connection](mssql.md#connection) for more details.

## Synapse Assets
### `synapse.sql`
Runs a materialized Synapse asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a view using view materialization
```bruin-sql
/* @bruin
name: customer_data.view
type: synapse.sql
materialization:
    type: view
@bruin */

select customer_id, first_name, last_name, email, country
from sales.customers
where active = 1
```

#### Example: Run a Synapse SQL script
```bruin-sql
/* @bruin
name: orders_summary
type: synapse.sql
@bruin */

create table temp_orders as
select
    order_id,
    order_date,
    customer_id,
    sum(quantity) as total_quantity,
    sum(price) as total_price
from sales.orders
group by order_id, order_date, customer_id;

create or replace view orders_summary as
select
    customer_id,
    count(order_id) as total_orders,
    sum(total_quantity) as total_quantity,
    sum(total_price) as total_price
from temp_orders
group by customer_id;
```

### `synapse.seed`
`synapse.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Synapse database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Synapse database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: synapse.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Synapse database

The examples below show how to load a CSV into a Synapse database.
```yaml
name: dashboard.hello
type: synapse.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
