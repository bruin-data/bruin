# Athena Assets
## athena.sql
Runs a materialized athena asset or an SQL script.
For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

### Examples
Create a view to aggregate website traffic data
```sql
/* @bruin
name: website_traffic.view
type: athena.sql
materialization:
    type: view
@bruin */

select
        date,
        count(distinct user_id) as unique_visitors,
        sum(page_views) as total_page_views,
        avg(session_duration) as avg_session_duration
        from raw_web_traffic
        group by date;

```

Create a table to analyze daily sales performance
```sql
/* @bruin
name: daily_sales_analysis.view
type: athena.sql
materialization:
    type: table
@bruin */

select
    order_date,
    sum(total_amount) as total_sales,
    count(distinct order_id) as total_orders,
    avg(total_amount) as avg_order_value
from sales_data
group by order_date;
```
