# AWS Redshift Assets
## rs.sql
Runs a materialized AWS Redshift asset or an SQL script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

### Examples
Create a table for product reviews
```sql
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
```

Run an AWS Redshift script to clean up old data
```sql
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
```
