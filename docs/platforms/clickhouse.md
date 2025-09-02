# Clickhouse
[Clickhouse](https://clickhouse.com/) is a high-performance, column-oriented SQL database management system for online analytical processing.
Bruin supports Clickhouse as both a source and a destination.

## Connection
In order to set up a Clickhouse connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
connections:
    clickhouse:
        - name: "connection_name"
          username: "clickhouse"
          password: "XXXXXXXXXX"
          host: "some-clickhouse-host.somedomain.com"   
          port: 9440
          database: "dev" #Optional for other assets. Ignored when using ClickHouse as an ingestr destination/source, as ingestr takes the database name from the asset file. 
          http_port: 8443 #Only specify if you are using ClickHouse as ingestr destination, by default it is 8443.
          secure: 1 #Required for ClickHouse Cloud
```

## Ingestr Assets:
After adding connection in `bruin.yml`. To ingest data to clickhouse, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., stripe_ingestion.yml) inside the assets folder and add the following content:
### 
```yaml
name: publicDB.stripe
type: ingestr

parameters:
  source_connection: stripe-default
  source_table: 'events'
  destination: clickhouse
```
In this case, the Clickhouse database is `publicDB`. Please ensure that the necessary permissions are granted to the user. For more details on obtaining credentials and setting up permissions, you can refer to this [guide](https://dlthub.com/docs/dlt-ecosystem/destinations/clickhouse#2-setup-clickhouse-database)


## Clickhouse Assets

### `clickhouse.sql`
Runs a materialized clickhouse asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

### Examples
Create a view to determine the top 10 earning drivers in a taxi company:
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

View Top 5 Customers by Spending:
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

Table with average driver rating:
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

### `clickhouse.sensor.query`

Checks if a query returns any results in Clickhouse, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
    poke_interval: int (optional)
```

**Parameters**:
- `query`: Query you expect to return any results
- `poke_interval`: The interval between retries in seconds (default 30 seconds).

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: clickhouse.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: clickhouse.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```


### `clickhouse.seed`
`clickhouse.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Clickhouse database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Clickhouse database.

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

The examples below show how to load a CSV into a Clickhouse database:
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
