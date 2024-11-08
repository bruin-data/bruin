# Microsoft SQL Server

Bruin supports Microsoft SQL Server as a data platform.

> [!NOTE]
> We tend to use "MS SQL" interchangeably to refer to Microsoft SQL Server, apologies for any confusion.


## Connection
In order to have set up a SQL Server connection on Bruin, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      mssql:
        - name: "connection_name"
          username: "mssql_user"
          password: "XXXXXXXXXX"
          host: "mssql_host.somedomain.com"
          port: 1433
          database: "dev"
```


## SQL Server Assets

### `ms.sql`
Runs a materialized SQL Server asset or an SQL script. For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

#### Examples
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
