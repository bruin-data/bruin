# Databricks Assets
## databricks.sql
Runs a materialized Databricks asset or an SQL script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

### Examples
Create a temporary view for top customers
```sql
/* @bruin
name: top_customers.view
type: databricks.sql
materialization:
    type: view
@bruin */

create temporary view top_customers as
select customer_id, sum(total_amount) as total_spent
from transactions
group by customer_id
order by total_spent desc
limit 100;
```

Run a Databricks script to update customer segmentation
```sql
/* @bruin
name: update_customer_segmentation
type: databricks.sql
@bruin */

create or replace table customer_segmentation as
select
    customer_id,
    case
        when total_spent > 1000 then 'Gold'
        when total_spent > 500 then 'Silver'
        else 'Bronze'
    end as segment
from (
    select customer_id, sum(amount) as total_spent
    from transactions
    group by customer_id
);
```
