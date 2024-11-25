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
