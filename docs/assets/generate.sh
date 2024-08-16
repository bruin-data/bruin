#!/bin/bash

# Create the markdown files with content

# Synapse
cat > synapse.md << EOL
# Synapse Assets
## synapse.sql
Runs a materialized Synapse asset or an SQL script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

### Examples
Create a view using view materialization
\`\`\`sql
/* @bruin
name: customer_data.view
type: synapse.sql
materialization:
    type: view
@bruin */

select customer_id, first_name, last_name, email, country
from sales.customers
where active = 1
\`\`\`

Run a Synapse SQL script
\`\`\`sql
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
\`\`\`
EOL
echo "Created synapse.md"

# MS SQL
cat > ms.md << EOL
# MS SQL Assets
## ms.sql
Runs a materialized MS SQL asset or an SQL script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

### Examples
Create a stored procedure for updating product prices
\`\`\`sql
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
\`\`\`

Run an MS SQL script to generate sales report
\`\`\`sql
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
\`\`\`
EOL
echo "Created ms.md"

# Databricks
cat > databricks.md << EOL
# Databricks Assets
## databricks.sql
Runs a materialized Databricks asset or an SQL script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

### Examples
Create a temporary view for top customers
\`\`\`sql
/* @bruin
name: top_customers.view
type: databricks.sql
materialization:
    type: temporary_view
@bruin */

create temporary view top_customers as
select customer_id, sum(total_amount) as total_spent
from transactions
group by customer_id
order by total_spent desc
limit 100;
\`\`\`

Run a Databricks script to update customer segmentation
\`\`\`sql
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
\`\`\`
EOL
echo "Created databricks.md"

# Redshift
cat > redshift.md << EOL
# AWS Redshift Assets
## rs.sql
Runs a materialized AWS Redshift asset or an SQL script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

### Examples
Create a table for product reviews
\`\`\`sql
/* @bruin
name: product_reviews.table
type: rs.sql
materialization:
    type: table
@bruin */

create table product_reviews (
    review_id bigint identity(1,1),
    product_id bigint,
    user_id bigint,
    rating int,
    review_text varchar(500),
    review_date timestamp
);
\`\`\`

Run an AWS Redshift script to clean up old data
\`\`\`sql
/* @bruin
name: clean_old_data
type: rs.sql
@bruin */

begin transaction;

delete from user_activity
where activity_date < dateadd(year, -2, current_date);

delete from order_history
where order_date < dateadd(year, -5, current_date);

commit transaction;
\`\`\`
EOL
echo "Created redshift.md"
