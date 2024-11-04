# MS SQL Assets
## ms.sql
Runs a materialized MS SQL asset or an SQL script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

### Examples
Create a stored procedure for updating product prices
```sql
/* @bruin
name: update_product_prices
type: ms.sql
materialization:
    type: stored_procedure
@bruin */

create procedure update_product_prices
as
begin
    update products
    set price = price * 1.1
    where category = 'electronics';
end
```

Run an MS SQL script to generate sales report
```sql
/* @bruin
name: sales_report
type: ms.sql
@bruin */

with monthly_sales as (
    select
        product_id,
        year(order_date) as order_year,
        month(order_date) as order_month,
        sum(quantity) as total_quantity,
        sum(price) as total_sales
    from sales.orders
    group by product_id, year(order_date), month(order_date)
)
select
    product_id,
    order_year,
    order_month,
    total_quantity,
    total_sales
from monthly_sales
order by order_year, order_month;
```