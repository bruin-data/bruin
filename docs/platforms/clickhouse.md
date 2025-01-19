# Clickhouse

Bruin supports [Clickhouse](https://clickhouse.com/) as a data platform so you can create [assets](../assets/definition-schema.md) that will result in tables and views in your clickhouse data warehouse

## Connection
In order to have set up a Clickhouse connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
connections:
    clickhouse:
        - name: "connection_name"
          username: "clickhouse"
          password: "XXXXXXXXXX"
          host: "some-clickhouse-host.somedomain.com"
          port: 9000
          database: "dev"
```

The field `database` is optional, if not provided, it will use the default database

## Clickhouse Assets

### `clickhouse.sql`
Runs a materialized clickhouse asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

### Examples
Create a view to determine the top 10 earning drivers in a taxi company
```bruin-sql
/* @bruin
name: highest_earning_drivers
type: clickhouse.sql
materialization:
    type: view
@bruin */

SELECT 
    driver_id, 
    SUM(fare_amount) AS total_earnings 
FROM trips 
GROUP BY driver_id 
ORDER BY total_earnings DESC 
LIMIT 10;
```

View Top 5 Customers by Spending
```bruin-sql
/* @bruin
name: top_five_customers
type: clickhouse.sql
materialization:
    type: view
@bruin */

SELECT 
    customer_id, 
    SUM(fare_amount) AS total_spent 
FROM trips 
GROUP BY customer_id 
ORDER BY total_spent DESC 
LIMIT 5;
```

Table with average driver rating
```bruin-sql
/* @bruin
name: average_Rating
type: clickhouse.sql
materialization:
    type: table
@bruin */

SELECT 
    driver_id, 
    AVG(rating) AS average_rating 
FROM trips 
GROUP BY driver_id 
ORDER BY average_rating DESC;
```


### `clickhouse.seed`
`clickhouse.seed` are a special type of assets that are used to represent are CSV-files that contain data that is prepared outside of your pipeline that will be loaded into your clickhouse database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the clickhouse database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: clickhouse.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Clickhouse database

The examples below show how load a csv into a clickhouse database.
```yaml
name: dashboard.hello
type: clickhouse.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```
